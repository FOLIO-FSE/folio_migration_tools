import json
import logging
import sys
from datetime import datetime, timezone
from typing import Dict, List, Set, Union
from uuid import uuid4

import i18n
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
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


class ItemMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        items_map,
        material_type_map,
        loan_type_map,
        location_map,
        call_number_type_map,
        holdings_id_map,
        statistical_codes_map,
        item_statuses_map,
        temporary_loan_type_mapping,
        temporary_location_mapping,
        library_configuration: LibraryConfiguration,
        task_configuration: AbstractTaskConfiguration,
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(
            folio_client,
            item_schema,
            items_map,
            statistical_codes_map,
            FOLIONamespaces.items,
            library_configuration,
            task_configuration,
        )
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map
        self.unique_barcodes: Set[str] = set()
        self.status_mapping: dict = {}
        if temporary_loan_type_mapping:
            self.temp_loan_type_mapping = RefDataMapping(
                self.folio_client,
                "/loan-types",
                "loantypes",
                temporary_loan_type_mapping,
                "name",
                "TemporaryLoanTypeMapping",
            )
        self.temp_location_mapping = None
        if temporary_location_mapping:
            self.temp_location_mapping = RefDataMapping(
                self.folio_client,
                "/locations",
                "locations",
                temporary_location_mapping,
                "code",
                "TemporaryLocationMapping",
            )

        if item_statuses_map:
            self.setup_status_mapping(item_statuses_map)
        if call_number_type_map:
            self.call_number_mapping = RefDataMapping(
                self.folio_client,
                "/call-number-types",
                "callNumberTypes",
                call_number_type_map,
                "name",
                "CallNumberTypeMapping",
            )
        self.loan_type_mapping = RefDataMapping(
            self.folio_client,
            "/loan-types",
            "loantypes",
            loan_type_map,
            "name",
            "PermanentLoanTypeMapping",
        )

        self.material_type_mapping = RefDataMapping(
            self.folio_client,
            "/material-types",
            "mtypes",
            material_type_map,
            "name",
            "MaterialTypeMapping",
        )

        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            "LocationMapping",
        )

    def perform_additional_mappings(
        self, legacy_ids: Union[str, List[str]], folio_rec: Dict, file_def: FileDefinition
    ):
        self.handle_suppression(folio_rec, file_def)
        self.map_statistical_codes(folio_rec, file_def)
        self.map_statistical_code_ids(legacy_ids, folio_rec)

    def handle_suppression(self, folio_record: Dict, file_def: FileDefinition):
        folio_record["discoverySuppress"] = file_def.discovery_suppressed
        self.migration_report.add(
            "Suppression",
            i18n.t("Suppressed from discovery") + f" = {folio_record['discoverySuppress']}",
        )

    def setup_status_mapping(self, item_statuses_map):
        statuses = self.item_schema["properties"]["status"]["properties"]["name"]["enum"]
        for mapping in item_statuses_map:
            if "folio_name" not in mapping:
                logging.critical("folio_name is not a column in the status mapping file")
                sys.exit(1)
            elif "legacy_code" not in mapping:
                logging.critical("legacy_code is not a column in the status mapping file")
                sys.exit(1)
            elif mapping["folio_name"] not in statuses:
                logging.critical(
                    "%s in the mapping file is not a FOLIO item status",
                    mapping["folio_name"],
                )
                sys.exit(1)
            elif mapping["legacy_code"] == "*":
                logging.critical(
                    "* in status mapping not allowed. Available will be the default mapping. "
                    "Please remove the row with the *"
                )
                sys.exit(1)
            elif not all(mapping.values()):
                logging.critical("empty value in mapping %s. Check mapping file", mapping.values())
                sys.exit(1)
            else:
                self.status_mapping = {
                    v["legacy_code"]: v["folio_name"] for v in item_statuses_map
                }
        logging.info(json.dumps(statuses, indent=True))

    def get_prop(self, legacy_item, folio_prop_name, index_or_id, schema_default_value):
        base_props = {
            "legacy_object": legacy_item,
            "index_or_id": index_or_id,
        }
        if folio_prop_name == "permanentLocationId":
            mapping_props = {
                **base_props,
                "ref_data_mapping": self.location_mapping,
                "prevent_default": self.task_configuration.prevent_permanent_location_map_default,
            }
            return self.get_mapped_ref_data_value(**mapping_props)
        elif folio_prop_name == "temporaryLocationId":
            if not self.temp_location_mapping:
                raise TransformationProcessError(
                    "Temporary location is mapped, but there is no "
                    "temporary location mapping file referenced in configuration"
                )
            mapping_props = {
                **base_props,
                "ref_data_mapping": self.temp_location_mapping,
                "prevent_default": True,
            }
            temp_loc = self.get_mapped_ref_data_value(**mapping_props)
            self.migration_report.add("TemporaryLocationMapping", f"{temp_loc}")
            return temp_loc
        elif folio_prop_name == "materialTypeId":
            mapping_props["ref_data_mapping"] = self.material_type_mapping
            return self.get_mapped_ref_data_value(**mapping_props)
        elif folio_prop_name == "itemLevelCallNumberTypeId":
            mapping_props["ref_data_mapping"] = self.call_number_mapping
            return self.get_mapped_ref_data_value(**mapping_props)
        elif folio_prop_name == "status.date":
            return datetime.now(timezone.utc).isoformat()
        elif folio_prop_name == "temporaryLoanTypeId":
            mapping_props = {
                **base_props,
                "ref_data_mapping": self.temp_loan_type_mapping,
                "prevent_default": True,
            }
            ltid = self.get_mapped_ref_data_value(**mapping_props)
            self.migration_report.add("TemporaryLoanTypeMapping", f"{folio_prop_name} -> {ltid}")
            return ltid
        elif folio_prop_name == "permanentLoanTypeId":
            mapping_props["ref_data_mapping"] = self.loan_type_mapping
            return self.get_mapped_ref_data_value(**mapping_props)

        mapped_value = super().get_prop(
            legacy_item, folio_prop_name, index_or_id, schema_default_value
        )
        if folio_prop_name == "status.name":
            return self.transform_status(mapped_value)
        elif folio_prop_name == "barcode":
            barcode = mapped_value
            normalized_barcode = barcode.strip().lower()
            if normalized_barcode and normalized_barcode in self.unique_barcodes:
                Helper.log_data_issue(index_or_id, "Duplicate barcode", mapped_value)
                self.migration_report.add_general_statistics(i18n.t("Duplicate barcodes"))
                return f"{barcode}-{uuid4()}"
            else:
                if normalized_barcode:
                    self.unique_barcodes.add(normalized_barcode)
                return barcode
        elif folio_prop_name == "holdingsRecordId":
            if mapped_value in self.holdings_id_map:
                return self.holdings_id_map[mapped_value][1]
            elif f"{self.bib_id_template}{mapped_value}" in self.holdings_id_map:
                return self.holdings_id_map[f"{self.bib_id_template}{mapped_value}"][1]
            self.migration_report.add_general_statistics(
                i18n.t("Records failed because of failed holdings"),
            )
            s = (
                "Holdings id referenced in legacy item "
                "was not found amongst transformed Holdings records"
            )
            raise TransformationRecordFailedError(index_or_id, s, mapped_value)
        elif mapped_value:
            return mapped_value
        else:
            self.migration_report.add("UnmappedProperties", f"{folio_prop_name}")
            return ""



    def transform_status(self, legacy_value):
        status = self.status_mapping.get(legacy_value, "Available")
        self.migration_report.add("StatusMapping", f"'{legacy_value}' -> {status}")
        return status
