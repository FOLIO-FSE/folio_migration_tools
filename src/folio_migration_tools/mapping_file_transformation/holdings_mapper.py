import ast

from folioclient import FolioClient
from folio_uuid.folio_uuid import FOLIONamespaces
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


class HoldingsMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        holdings_map,
        location_map,
        call_number_type_map,
        instance_id_map,
        library_configuration: LibraryConfiguration,
        statistical_codes_map=None,
    ):
        holdings_schema = folio_client.get_holdings_schema()
        self.instance_id_map = instance_id_map
        super().__init__(
            folio_client,
            holdings_schema,
            holdings_map,
            statistical_codes_map,
            FOLIONamespaces.holdings,
            library_configuration,
        )
        self.holdings_map = holdings_map

        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            Blurbs.LocationMapping,
        )
        if call_number_type_map:
            self.call_number_mapping = RefDataMapping(
                self.folio_client,
                "/call-number-types",
                "callNumberTypes",
                call_number_type_map,
                "name",
                Blurbs.CallNumberTypeMapping,
            )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        if not self.use_map:
            return legacy_item[folio_prop_name]
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        # IF there is a value mapped, return that one
        if len(legacy_item_keys) == 1 and folio_prop_name in self.mapped_from_values:
            value = self.mapped_from_values.get(folio_prop_name, "")
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value
        legacy_values = MappingFileMapperBase.get_legacy_vals(
            legacy_item, legacy_item_keys
        )
        legacy_value = " ".join(legacy_values).strip()
        if folio_prop_name == "permanentLocationId":
            return self.get_location_id(legacy_item, index_or_id, folio_prop_name)
        elif folio_prop_name == "temporaryLocationId":
            return self.get_location_id(legacy_item, index_or_id, folio_prop_name, True)
        elif folio_prop_name == "callNumber":
            if legacy_value.startswith("[") and len(legacy_value.split(",")) > 1:
                self.migration_report.add_general_statistics(
                    "Bound-with items callnumber identified"
                )
                self.migration_report.add(
                    Blurbs.BoundWithMappings,
                    (
                        f"Number of bib-level callnumbers in record: "
                        f"{len(legacy_value.split(','))}"
                    ),
                )
            return legacy_value.removeprefix("[").removesuffix("]")
        elif folio_prop_name == "callNumberTypeId":
            return self.get_call_number_type_id(
                legacy_item, folio_prop_name, index_or_id
            )
        elif folio_prop_name == "statisticalCodeIds":
            return self.get_statistical_codes(
                legacy_values, folio_prop_name, index_or_id
            )
        elif folio_prop_name == "instanceId":
            return self.get_instance_ids(legacy_value, index_or_id)
        elif any(legacy_item_keys):
            return legacy_value
        else:
            # edge case
            return ""

    def get_location_id(
        self, legacy_item: dict, id_or_index, folio_prop_name, prevent_default=False
    ):
        return self.get_mapped_value(
            self.location_mapping,
            legacy_item,
            id_or_index,
            folio_prop_name,
            prevent_default,
        )

    def get_call_number_type_id(self, legacy_item, folio_prop_name: str, id_or_index):
        if self.call_number_mapping:
            return self.get_mapped_value(
                self.call_number_mapping, legacy_item, id_or_index, folio_prop_name
            )
        self.migration_report.add(Blurbs.CallNumberTypeMapping, "No mapping")
        return ""

    def get_instance_ids(self, legacy_value: str, index_or_id: str):
        # Returns a list of Id:s
        return_ids = []
        legacy_bib_ids = self.get_legacy_bib_ids(legacy_value, index_or_id)
        self.migration_report.add(
            Blurbs.BoundWithMappings,
            f"Number of bib records referenced in item: {len(legacy_bib_ids)}",
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
                    "Records not matched to Instances"
                )
                s = "Bib id not in instance id map."
                raise TransformationRecordFailedError(index_or_id, s, new_legacy_value)
            else:
                self.migration_report.add_general_statistics(
                    "Records matched to Instances"
                )
                entry = self.instance_id_map.get(
                    new_legacy_value, ""
                ) or self.instance_id_map.get(legacy_instance_id)
                return_ids.append(entry["folio_id"])
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
                    "Bound-with items identified by bib id"
                )
                self.migration_report.add(
                    Blurbs.GeneralStatistics,
                    "Bib ids referenced in bound-with items",
                    new_value_len,
                )
            return new_legacy_values
        except Exception as error:
            raise TransformationRecordFailedError(
                index_or_id,
                f"Instance ID could not get parsed to array of strings {error}",
                legacy_value,
            )
