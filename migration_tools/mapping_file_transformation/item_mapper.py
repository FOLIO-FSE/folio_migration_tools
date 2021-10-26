import json
import logging
import sys
from datetime import datetime
from typing import Dict, List

from folioclient import FolioClient
from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
from migration_tools.report_blurbs import Blurbs


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
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(folio_client, item_schema, items_map, statistical_codes_map)
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map

        self.ids_dict: Dict[str, set] = {}
        self.use_map = True
        self.status_mapping = {}
        if temporary_loan_type_mapping:
            self.temp_loan_type_mapping = RefDataMapping(
                self.folio_client,
                "/loan-types",
                "loantypes",
                temporary_loan_type_mapping,
                "name",
            )
        if temporary_location_mapping:
            self.temp_location_mapping = RefDataMapping(
                self.folio_client,
                "/locations",
                "locations",
                temporary_location_mapping,
                "code",
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
            )
        self.loan_type_mapping = RefDataMapping(
            self.folio_client, "/loan-types", "loantypes", loan_type_map, "name"
        )

        self.material_type_mapping = RefDataMapping(
            self.folio_client, "/material-types", "mtypes", material_type_map, "name"
        )

        self.location_mapping = RefDataMapping(
            self.folio_client, "/locations", "locations", location_map, "code"
        )

    def perform_additional_mappings(self):
        raise NotImplementedError()

    def setup_status_mapping(self, item_statuses_map):
        statuses = self.item_schema["properties"]["status"]["properties"]["name"][
            "enum"
        ]
        for mapping in item_statuses_map:
            if "folio_name" not in mapping:
                logging.critical(
                    "folio_name is not a column in the status mapping file"
                )
                sys.exit()
            elif "legacy_code" not in mapping:
                logging.critical(
                    "legacy_code is not a column in the status mapping file"
                )
                sys.exit()
            elif mapping["folio_name"] not in statuses:
                logging.critical(
                    f'{mapping["folio_name"]} in the mapping file is not a FOLIO item status'
                )
                sys.exit()
            elif mapping["legacy_code"] == "*":
                logging.critical(
                    "* in status mapping not allowed. Available will be the default mapping. "
                    "Please remove the row with the *"
                )
                sys.exit()
            elif not all(mapping.values()):
                logging.critical(
                    f"empty value in mapping {mapping.values()}. Check mapping file"
                )
                sys.exit()
            else:
                self.status_mapping = {
                    v["legacy_code"]: v["folio_name"] for v in item_statuses_map
                }
        logging.info(json.dumps(statuses, indent=True))

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        value_tuple = (legacy_item, folio_prop_name, index_or_id)
        if not self.use_map:
            return legacy_item[folio_prop_name]
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        legacy_values = MappingFileMapperBase.get_legacy_vals(
            legacy_item, legacy_item_keys
        )
        legacy_value = " ".join(legacy_values).strip()
        if folio_prop_name == "permanentLocationId":
            return self.get_mapped_value(
                self.location_mapping,
                *value_tuple,
                False,
            )
        elif folio_prop_name == "temporaryLocationId":
            temp_loc = self.get_mapped_value(
                self.temp_location_mapping,
                *value_tuple,
                True,
            )
            self.migration_report.add(Blurbs.TemporaryLocationMapping, f"{temp_loc}")
            return temp_loc
        elif folio_prop_name == "materialTypeId":
            return self.get_mapped_value(
                self.material_type_mapping,
                *value_tuple,
            )
        elif folio_prop_name == "itemLevelCallNumberTypeId":
            return self.get_item_level_call_number_type_id(
                legacy_item, folio_prop_name, index_or_id
            )
        elif folio_prop_name == "status.name":
            return self.transform_status(legacy_value)
        elif folio_prop_name == "barcode":
            return next((v for v in legacy_values if v), "")
        elif folio_prop_name == "status.date":
            return datetime.utcnow().isoformat()
        elif folio_prop_name == "temporaryLoanTypeId":
            ltid = self.get_mapped_value(
                self.temp_loan_type_mapping,
                *value_tuple,
                True,
            )
            self.migration_report.add(
                Blurbs.TemporaryLoanTypeMapping, f"{folio_prop_name} -> {ltid}"
            )
            return ltid
        elif folio_prop_name == "permanentLoanTypeId":
            return self.get_mapped_value(self.loan_type_mapping, *value_tuple)
        elif folio_prop_name.startswith("statisticalCodeIds"):
            statistical_code_id = self.get_statistical_codes(
                legacy_item, folio_prop_name, index_or_id
            )
            self.migration_report.add(
                Blurbs.StatisticalCodeMapping,
                f"{folio_prop_name} -> {statistical_code_id}",
            )
            return statistical_code_id
        elif folio_prop_name == "holdingsRecordId":
            if legacy_value not in self.holdings_id_map:
                self.migration_report.add_general_statistics(
                    "Records failed because of failed holdings",
                )
                self.migration_report.add_general_statistics(
                    "Items linked to a Holdingsrecord"
                )
                s = (
                    "Holdings id referenced in legacy item "
                    "was not found amongst transformed Holdings records"
                )
                raise TransformationRecordFailedError(index_or_id, s, legacy_value)
            else:
                return self.holdings_id_map[legacy_value]["id"]
        elif len(legacy_item_keys) == 1 or folio_prop_name in self.mapped_from_values:
            value = self.mapped_from_values.get(folio_prop_name, "")
            if value not in [None, ""]:
                return value
            else:
                return legacy_value
        elif any(legacy_item_keys):
            return legacy_value
        else:
            self.migration_report.add(
                Blurbs.UnmappedProperties, f"{folio_prop_name} {legacy_item_keys}"
            )
            return ""

    def get_item_level_call_number_type_id(
        self, legacy_item, folio_prop_name: str, index_or_id
    ):
        if self.call_number_mapping:
            return self.get_mapped_value(
                self.call_number_mapping, legacy_item, index_or_id, folio_prop_name
            )
        self.migration_report.add(
            Blurbs.CallNumberTypeMapping,
            "Mapping not setup",
        )
        return ""

    def transform_status(self, legacy_value):
        status = self.status_mapping.get(legacy_value, "Available")
        self.migration_report.add(Blurbs.StatusMapping, f"'{legacy_value}' -> {status}")
        return status
