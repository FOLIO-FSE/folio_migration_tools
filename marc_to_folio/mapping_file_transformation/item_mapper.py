import csv
import json
import logging
from marc_to_folio.mapping_file_transformation.ref_data_mapping import RefDataMapping
from marc_to_folio.custom_exceptions import (
    TransformationCriticalDataError,
    TransformationProcessError,
)
from typing import List
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
import re
import uuid
from datetime import datetime, time

from typing import Dict

from folioclient import FolioClient


class ItemMapper(MapperBase):
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
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(folio_client, item_schema, items_map)
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map        
        self.holdings_id_map = holdings_id_map
        self.set_to_migration_report(
            "Holdings IDs mapped", f"Unique holdings", len(self.holdings_id_map)
        )
        self.ids_dict: Dict[str, set] = {}
        self.use_map = True
        self.status_mapping = {}
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
        if statistical_codes_map:
            self.statistical_codes_mapping = RefDataMapping(
                self.folio_client,
                "/statistical-codes",
                "statisticalCodes",
                statistical_codes_map,
                "code",
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
                    f"folio_name is not a column in the status mapping file"
                )
                exit()
            elif "legacy_code" not in mapping:
                logging.critical(
                    f"legacy_code is not a column in the status mapping file"
                )
                exit()
            elif mapping["folio_name"] not in statuses:
                logging.critical(
                    f'{mapping["folio_name"]} in the mapping file is not a FOLIO item status'
                )
                exit()
            elif mapping["legacy_code"] == "*":
                logging.critical(
                    f"* in status mapping not allowed. Available will be the default mapping. "
                    "Please remove the row with the *"
                )
                exit()
            elif not all(mapping.values()):
                logging.critical(
                    f"empty value in mapping {mapping.values()}. Check mapping file"
                )
                exit()
            else:
                self.status_mapping = {
                    v["legacy_code"]: v["folio_name"] for v in item_statuses_map
                }
        logging.info(json.dumps(statuses, indent=True))

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        # logging.debug(f"get item prop {folio_prop_name}")
        if self.use_map:
            # legacy_item_keys = [k["legacy_field"] for k in self.items_map["data"]
            #            if k["folio_field"] == folio_prop_name]
            # vals = [v for k, v in legacy_item.items() if k in legacy_item_keys]
            legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
            # legacy_value = ""
            legacy_values = MapperBase.get_legacy_vals(legacy_item, legacy_item_keys)
            legacy_value = " ".join(legacy_values).strip()
            if folio_prop_name == "permanentLocationId":                
                return self.get_location_id(legacy_item, index_or_id)
            elif folio_prop_name == "temporaryLocationId":
                if len(legacy_values) == 1:
                    t = self.location_mapping.get_ref_data_tuple(legacy_value)
                    # raise NotImplementedError("Temporary locations does not get mapped")
                    if t and t[0] != self.location_mapping.default_id:
                        self.add_to_migration_report("Temporary locations mapping",f"{legacy_value} -> {t[1]}")
                        return t[0]
                self.add_to_migration_report("Temporary locations mapping",f"Unmapped ({legacy_value})")
                return ""
            elif folio_prop_name == "materialTypeId":
                return self.get_material_type_id(legacy_item)
            elif folio_prop_name == "itemLevelCallNumberTypeId":
                self.report_folio_mapping(f"{folio_prop_name}", True)
                return self.get_item_level_call_number_type_id(legacy_item)
            elif folio_prop_name == "status.name":
                self.report_folio_mapping(f"{folio_prop_name}", True)
                return self.transform_status(legacy_value)
            elif folio_prop_name == "barcode":
                return next((v for v in legacy_values if v), "")
            elif folio_prop_name == "status.date":
                return datetime.utcnow().isoformat()
            elif folio_prop_name in ["permanentLoanTypeId", "temporaryLoanTypeId"]:
                return self.get_loan_type_id(legacy_item)
            elif folio_prop_name == "statisticalCodeIds":
                return self.get_statistical_codes(legacy_item)
            elif folio_prop_name == "holdingsRecordId":
                if legacy_value not in self.holdings_id_map:
                    self.add_to_migration_report(
                        "General statistics",
                        "Records failed because of failed holdings",
                    )
                    self.add_to_migration_report("Holdings IDs mapped", f"Unmapped")
                    s = f"Holdings id '{legacy_value}' not in hold id map."
                    raise TransformationProcessError(s, index_or_id)
                else:
                    self.add_to_migration_report("Holdings IDs mapped", f"Mapped")
                    return self.holdings_id_map[legacy_value]["id"]
            elif (
                len(legacy_item_keys) == 1 or folio_prop_name in self.mapped_from_values
            ):
                value = self.mapped_from_values.get(folio_prop_name, "")
                if value not in [None, ""]:
                    return value
                else:
                    return legacy_value
            elif any(legacy_item_keys):
                return legacy_value
            else:
                self.add_to_migration_report(
                    "Unmapped properties", f"{folio_prop_name} {legacy_item_keys}"
                )
                return ""
        else:
            self.report_folio_mapping(f"{folio_prop_name}", True, False)
            return legacy_item[folio_prop_name]

    def get_statistical_codes(self, legacy_item: dict):
        if self.statistical_codes_mapping:
            return self.get_mapped_value(self.statistical_code_mapping, legacy_item)
        self.add_to_migration_report(
            "Statistical code mapping",
            "Mapping not setup",
        )
        return ""

    def get_loan_type_id(self, legacy_item: dict):
        return self.get_mapped_value(self.loan_type_mapping, legacy_item)

    def get_material_type_id(self, legacy_item: dict):
        logging.debug(f"Material type mapping")
        return self.get_mapped_value(self.material_type_mapping, legacy_item)

    def get_location_id(self, legacy_item: dict, id_or_index, prevent_default=False):
        return self.get_mapped_value(
            self.location_mapping, legacy_item, prevent_default
        )

    def get_item_level_call_number_type_id(self, legacy_item):
        if self.call_number_type_map:
            return self.get_mapped_value(self.call_number_mapping, legacy_item)
        self.add_to_migration_report(
            "Callnumber type mapping",
            "Mapping not setup",
        )
        return ""

    def transform_status(self, legacy_value):
        status = self.status_mapping.get(legacy_value, "Available")
        self.add_to_migration_report("Status mapping", f"{legacy_value} -> {status}")
        return status
