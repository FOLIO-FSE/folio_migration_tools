"""Mapper for transforming holdings data to FOLIO Holdings format.

Provides the HoldingsMapper class for mapping legacy holdings data to FOLIO Holdings
records using configured mapping files. Handles locations, call numbers, notes, and
bound-with relationships.
"""

import ast
import json
import logging

import i18n
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class HoldingsMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        holdings_map,
        location_map,
        call_number_type_map,
        instance_id_map,        
        library_configuration: LibraryConfiguration,
        task_config: AbstractTaskConfiguration,
        statistical_codes_map=None,
        call_number_type_blurb_id="CallNumberTypeMapping",
    ):
        """Initialize HoldingsMapper for holdings transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            holdings_map: Mapping configuration for holdings fields.
            location_map: Mapping of legacy to FOLIO locations.
            call_number_type_map: Mapping of legacy to FOLIO call number types.
            instance_id_map: Mapping of legacy to FOLIO instance IDs.
            library_configuration (LibraryConfiguration): Library configuration.
            task_config (AbstractTaskConfiguration): Task configuration.
            statistical_codes_map: Mapping of legacy to FOLIO statistical codes.
        """
        holdings_schema = folio_client.get_holdings_schema()
        self.instance_id_map = instance_id_map
        super().__init__(
            folio_client,
            holdings_schema,
            holdings_map,
            statistical_codes_map,
            FOLIONamespaces.holdings,
            library_configuration,
            task_config,
        )
        self.holdings_map = holdings_map

        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            "LocationMapping",
        )
        self.call_number_mapping = RefDataMapping(
            self.folio_client,
            "/call-number-types",
            "callNumberTypes",
            call_number_type_map,
            "name",
            call_number_type_blurb_id,
        )

        self.holdings_sources = self.get_holdings_sources()
        call_number_types = self.folio_client.folio_get_all(
            "/call-number-types", "callNumberTypes", "", 1000
        )
        self.call_number_types_by_name = {c["name"]: c["id"] for c in call_number_types}

    def get_holdings_sources(self):
        res = {}
        holdings_sources = list(
            self.folio_client.folio_get_all("/holdings-sources", "holdingsRecordsSources")
        )
        logging.info("Fetched %s holdingsRecordsSources from tenant", len(holdings_sources))
        res = {n["name"].upper(): n["id"] for n in holdings_sources}
        if "FOLIO" not in res:
            raise TransformationProcessError("", "No holdings source with name FOLIO in tenant")
        if "MARC" not in res:
            raise TransformationProcessError("", "No holdings source with name MARC in tenant")
        logging.info(json.dumps(res, indent=4))
        return res

    def perform_additional_mappings(self, legacy_ids, folio_rec, file_def):
        self.handle_suppression(folio_rec, file_def)
        self.map_statistical_codes(folio_rec, file_def)
        self.map_statistical_code_ids(legacy_ids, folio_rec)
        self.handle_default_call_number_type(folio_rec)

    def handle_default_call_number_type(self, folio_record: dict):
        if "callNumberTypeId" in folio_record or "callNumber" not in folio_record:
            return
        elif self.task_configuration.default_call_number_type_name:
            default_name = self.task_configuration.default_call_number_type_name
            try:
                default_uuid = self.call_number_types_by_name[default_name]
                folio_record["callNumberTypeId"] = default_uuid
            except KeyError:
                raise TransformationProcessError(
                    "",
                    f"{default_name} is not a configured Call Number Type in this tenant",
                )

    def handle_suppression(self, folio_record, file_def: FileDefinition):
        folio_record["discoverySuppress"] = file_def.discovery_suppressed
        self.migration_report.add(
            "Suppression",
            i18n.t("Suppressed from discovery") + f" = {folio_record['discoverySuppress']}",
        )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id, schema_default_value):
        if folio_prop_name == "permanentLocationId":
            return self.get_mapped_ref_data_value(
                ref_data_mapping=self.location_mapping,
                legacy_object=legacy_item,
                index_or_id=index_or_id,
                prevent_default=False,
            )
        elif folio_prop_name == "callNumberTypeId":
            value = self.get_mapped_ref_data_value(
                ref_data_mapping=self.call_number_mapping,
                legacy_object=legacy_item,
                index_or_id=index_or_id,
                prevent_default=True,
            )
            return value
        # elif folio_prop_name.startswith("statisticalCodeIds"):
        #     return self.get_statistical_code(legacy_item, folio_prop_name, index_or_id)

        mapped_value = super().get_prop(
            legacy_item, folio_prop_name, index_or_id, schema_default_value
        )
        if folio_prop_name == "callNumber":
            return self.get_call_number(mapped_value)
        elif folio_prop_name == "instanceId":
            return self.get_instance_ids(mapped_value, index_or_id)
        elif mapped_value:
            return mapped_value
        else:
            self.migration_report.add("UnmappedProperties", f"{folio_prop_name}")
            return ""

    def get_call_number(self, legacy_value):
        if legacy_value.startswith("[") and len(legacy_value.split(",")) > 1:
            self.migration_report.add_general_statistics(
                i18n.t("Bound-with items callnumber identified")
            )
            self.migration_report.add(
                "BoundWithMappings",
                (f"Number of bib-level callnumbers in record: {len(legacy_value.split(','))}"),
            )
        if legacy_value.startswith("[") and len(legacy_value.split(",")) == 1:
            try:
                legacy_value = ast.literal_eval(str(legacy_value))[0]
            except (SyntaxError, ValueError):
                return legacy_value
        return legacy_value

    def get_instance_ids(self, legacy_value: str, index_or_id: str):
        # Returns a list of Id:s
        return_ids = []
        legacy_bib_ids = self.get_legacy_bib_ids(legacy_value, index_or_id)
        self.migration_report.add(
            "BoundWithMappings",
            i18n.t("Number of bib records referenced in item") + f": {len(legacy_bib_ids)}",
        )
        for legacy_instance_id in legacy_bib_ids:
            new_legacy_value = (
                f".{legacy_instance_id}"
                if legacy_instance_id.startswith("b")
                else legacy_instance_id
            )
            if (
                new_legacy_value not in self.instance_id_map
                and legacy_instance_id not in self.instance_id_map
            ):
                self.migration_report.add_general_statistics(
                    i18n.t("Records not matched to Instances")
                )
                s = "Bib id not in instance id map."
                raise TransformationRecordFailedError(index_or_id, s, new_legacy_value)
            else:
                self.migration_report.add_general_statistics(
                    i18n.t("Records matched to Instances")
                )
                entry = self.instance_id_map.get(new_legacy_value, "") or self.instance_id_map.get(
                    legacy_instance_id
                )
                return_ids.append(entry[1])
        if any(return_ids):
            return return_ids
        else:
            raise TransformationRecordFailedError(
                index_or_id, "No instance id mapped from", legacy_value
            )

    def get_legacy_bib_ids(self, legacy_value: str, index_or_id: str) -> list[str]:
        if not legacy_value.startswith("["):
            return [legacy_value]
        try:
            new_legacy_values = ast.literal_eval(legacy_value)
            new_value_len = len(new_legacy_values)
            if new_value_len > 1:
                self.migration_report.add_general_statistics(
                    i18n.t("Bound-with items identified by bib id")
                )
                self.migration_report.add(
                    "GeneralStatistics",
                    i18n.t("Bib ids referenced in bound-with items"),
                    new_value_len,
                )
            return new_legacy_values
        except Exception as error:
            raise TransformationRecordFailedError(
                index_or_id,
                f"Instance ID could not get parsed to array of strings {error}",
                legacy_value,
            ) from error
