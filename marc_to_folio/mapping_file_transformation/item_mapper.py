import csv
import json
import logging
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
        error_file,
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(folio_client, item_schema, items_map, error_file)
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map

        self.ids_dict: Dict[str, set] = {}
        self.use_map = True
        if call_number_type_map:
            self.call_number_type_map = call_number_type_map
            self.call_number_type_keys = []
            self.default_call_number_type_id = ""
            self.setup_call_number_type_mappings()

        if statistical_codes_map:
            self.statistical_codes_mappings = statistical_codes_map
            self.statistical_codes_keys = []
            self.setup_statistical_codes_mappings()

        self.loan_type_map = loan_type_map
        self.default_loan_type_id = ""
        self.loan_type_keys = []
        self.setup_loan_type_mappings()

        self.material_type_map = material_type_map
        self.default_material_type_id = ""
        self.material_type_keys = []
        self.setup_material_type_mappings()

        self.location_map = location_map
        self.location_keys = []
        self.default_location_id = ""
        self.setup_location_mappings(location_map)

    def perform_additional_mappings(self):
        raise NotImplementedError()

    def get_prop(self, legacy_item, folio_prop_name, index_or_id, i=0):
        # logging.debug(f"get item prop {folio_prop_name}")
        if self.use_map:
            formatted_prop_name = f"{folio_prop_name}[{i}]"
            legacy_item_keys = [
                k["legacy_field"]
                for k in self.items_map["data"]
                if k["folio_field"] in [folio_prop_name, formatted_prop_name]
            ]

            legacy_value = ""
            vals = [legacy_item[k] for k in legacy_item_keys if legacy_item.get(k, "") not in ["", None]]
            # vals = [v for k, v in legacy_item.items() if k in legacy_item_keys]
            legacy_value = " ".join(vals).strip()
            self.add_to_migration_report("Source fields with same target", len(vals))
            if folio_prop_name in ["permanentLocationId", "temporaryLocationId"]:
                return self.get_location_id(legacy_item, index_or_id)
            elif folio_prop_name == "materialTypeId":
                mt = self.get_material_type_id(legacy_item)
                if not mt:
                    raise TransformationCriticalDataError(
                        f"material Type id not mapped. {legacy_item_keys} - {vals}"
                    )
                return mt
            elif folio_prop_name == "itemLevelCallNumberTypeId":
                return self.get_item_level_call_number_type_id(legacy_item)
            elif folio_prop_name == "status.name":
                return self.transform_status(legacy_value)
            elif folio_prop_name == "barcode":
                print(legacy_item_keys)
                return next((v for v in vals if v), "")
            elif folio_prop_name == "status.date":
                return datetime.utcnow().isoformat()
            elif folio_prop_name in ["permanentLoanTypeId", "temporaryLoanTypeId"]:
                return self.get_loan_type_id(legacy_item)
            elif folio_prop_name == "statisticalCodeIds":
                return self.get_statistical_codes(legacy_item)
            elif folio_prop_name == "holdingsRecordId":
                if legacy_value not in self.holdings_id_map:
                    self.add_stats("Records failed because of failed holdings")
                    self.add_to_migration_report("Holdings IDs mapped", f"Unmapped")
                    s = f"Holdings id '{legacy_value}' not in hold id map."
                    raise TransformationProcessError(s, index_or_id)
                else:
                    self.add_to_migration_report("Holdings IDs mapped", f"Mapped")
                    return self.holdings_id_map[legacy_value]["id"]
            elif len(legacy_item_keys) == 1:
                value = next(
                    (
                        k.get("value", "")
                        for k in self.items_map["data"]
                        if k["folio_field"] == folio_prop_name
                    ),
                    "",
                )
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
        return self.get_mapped_value(
            "Statistical codes",
            legacy_item,
            self.statistical_codes_keys,
            self.statistical_codes_mappings,
            "",
            "folio_code",
        )

    def get_loan_type_id(self, legacy_item: dict):
        return self.get_mapped_value(
            "Loan type",
            legacy_item,
            self.loan_type_keys,
            self.loan_type_map,
            self.default_loan_type_id,
            "folio_name",
        )

    def get_material_type_id(self, legacy_item: dict):
        return self.get_mapped_value(
            "Material type",
            legacy_item,
            self.material_type_keys,
            self.material_type_map,
            self.default_material_type_id,
            "folio_name",
        )

    def get_location_id(self, legacy_item: dict, id_or_index):
        return self.get_mapped_value(
            "Location",
            legacy_item,
            self.location_keys,
            self.location_map,
            self.default_location_id,
            "folio_code",
        )

    def get_item_level_call_number_type_id(self, legacy_item):
        if self.call_number_type_map:
            return self.get_mapped_value(
                "Callnumber type",
                legacy_item,
                self.call_number_type_keys,
                self.call_number_type_map,
                self.default_call_number_type_id,
                "folio_name",
            )
        self.add_to_migration_report(
            "Callnumber type mapping",
            "Mapping not setup",
        )
        return ""

    def transform_status(self, legacy_value):
        self.add_to_migration_report("Status mapping", f"{legacy_value} -> Available")
        return "Available"

    def setup_statistical_codes_mappings(self):
        # Loan types
        logging.info("Fetching statistical codes...")
        self.statistical_codes = list(
            self.folio_client.folio_get_all("/statistical-codes", "statisticalCodes")
        )
        for idx, statistical_codes_mapping in enumerate(
            self.statistical_codes_mappings
        ):
            try:
                if idx == 1:
                    self.statistical_codes_keys = [
                        k
                        for k in statistical_codes_mapping.keys()
                        if k not in ["folio_code", "folio_id", "folio_name"]
                    ]
                # No default. Do  not return any if not set/mapped
                statistical_codes_mapping["folio_id"] = self.get_ref_data_tuple_by_code(
                    self.statistical_codes,
                    "statistical_codes",
                    statistical_codes_mapping["folio_code"],
                )[0]
            except TransformationProcessError as te:
                raise te
            except Exception:
                logging.info(json.dumps(self.statistical_codes_mappings, indent=4))
                raise TransformationProcessError(
                    f"{statistical_codes_mapping['folio_code']} could not be found in FOLIO"
                )
        logging.info(
            f"loaded {idx} mappings for {len(self.statistical_codes)} statistical codes in FOLIO"
        )

    def setup_call_number_type_mappings(self):
        # Loan types
        logging.info("Fetching Callnumber types...")
        self.folio_call_number_types = list(
            self.folio_client.folio_get_all("/call-number-types", "callNumberTypes")
        )
        for idx, call_number_type_mapping in enumerate(self.call_number_type_map):
            try:
                if idx == 1:
                    self.call_number_type_keys = [
                        k
                        for k in call_number_type_mapping.keys()
                        if k not in ["folio_code", "folio_id", "folio_name"]
                    ]
                    logging.info(json.dumps(self.call_number_type_keys, indent=4))
                if any(m for m in call_number_type_mapping.values() if m == "*"):
                    # Set up default mapping if available
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_call_number_types,
                        "callnumbers",
                        call_number_type_mapping["folio_name"],
                    )
                    if t:
                        self.default_call_number_type_id = t[0]
                        logging.info(
                            f'Set {call_number_type_mapping["folio_name"]} as default call_numbertype mapping'
                        )
                    else:
                        x = call_number_type_mapping.get("folio_name", "")
                        raise TransformationProcessError(
                            f"No Default call_number type -{x}- set up in map. "
                            "Add a row to mapping file with *:s and a valid call_number type"
                        )
                else:
                    call_number_type_mapping[
                        "folio_id"
                    ] = self.get_ref_data_tuple_by_name(
                        self.folio_call_number_types,
                        "callnumbers",
                        call_number_type_mapping["folio_name"],
                    )[
                        0
                    ]
            except TransformationProcessError as te:
                raise te
            except Exception:
                logging.info(json.dumps(self.call_number_type_map, indent=4))
                raise TransformationProcessError(
                    f"{call_number_type_mapping['folio_name']} could not be found in FOLIO"
                )
        if not self.default_call_number_type_id:
            raise TransformationProcessError(
                "No Default Callnumber type set up in map."
                "Add a row to mapping file with *:s and a valid callnumber type"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.folio_call_number_types)} call number types in FOLIO"
        )

    def setup_loan_type_mappings(self):
        # Loan types
        logging.info("Fetching Loan types...")
        self.folio_loan_types = list(
            self.folio_client.folio_get_all("/loan-types", "loantypes")
        )
        for idx, loan_type_mapping in enumerate(self.loan_type_map):
            try:
                if idx == 1:
                    self.loan_type_keys = [
                        k
                        for k in loan_type_mapping.keys()
                        if k
                        not in [
                            "folio_code",
                            "folio_id",
                            "folio_name",
                            "legacy_code",
                        ]
                    ]
                if any(m for m in loan_type_mapping.values() if m == "*"):
                    # Set up default mapping if available
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_loan_types,
                        "loan_types",
                        loan_type_mapping["folio_name"],
                    )
                    if not t:
                        raise TransformationProcessError(
                            "No Default Loan type set up in map."
                            "Add a row to mapping file with *:s and a valid loan type"
                        )
                    self.default_loan_type_id = t[0]
                    logging.info(
                        f'Set {loan_type_mapping["folio_name"]} as default Loantype mapping'
                    )
                else:
                    loan_type_mapping["folio_id"] = self.get_ref_data_tuple_by_name(
                        self.folio_loan_types,
                        "loan_types",
                        loan_type_mapping["folio_name"],
                    )[0]
            except TransformationProcessError as te:
                raise te
            except Exception:
                logging.info(json.dumps(self.loan_type_map, indent=4))
                raise TransformationProcessError(
                    f"{loan_type_mapping['folio_name']} could not be found in FOLIO"
                )
        if not self.default_loan_type_id:
            raise TransformationProcessError(
                "No Default Loan type set up in map."
                "Add a row to mapping file with *:s and a valid loan type"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.folio_loan_types)} loan types in FOLIO"
        )

    def setup_material_type_mappings(self):
        # Material types
        logging.info("Fetching Material types...")
        self.folio_material_types = list(
            self.folio_client.folio_get_all("/material-types", "mtypes")
        )
        for idx, mat_mapping in enumerate(self.material_type_map):
            try:
                if idx == 1:
                    self.material_type_keys = [
                        k
                        for k in mat_mapping.keys()
                        if k
                        not in [
                            "folio_code",
                            "folio_id",
                            "folio_name",
                            "legacy_code",
                        ]
                    ]
                if any(m for m in mat_mapping.values() if m == "*"):
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_material_types,
                        "mat_types",
                        mat_mapping["folio_name"],
                    )
                    if not t:
                        raise TransformationProcessError(
                            "No Default Material type set up in map."
                            "Add a row to mapping file with *:s and a valid Material type"
                        )
                    self.default_material_type_id = t[0]
                    logging.info(
                        f'Set {mat_mapping["folio_name"]} as default material type mapping'
                    )
                else:
                    mat_mapping["folio_id"] = self.get_ref_data_tuple_by_name(
                        self.folio_material_types,
                        "mat_types",
                        mat_mapping["folio_name"],
                    )[0]
            except TransformationProcessError as te:
                raise te
            except Exception:
                raise Exception(
                    f"{mat_mapping['folio_name']} could not be found in FOLIO"
                )
        if not self.default_material_type_id:
            raise TransformationProcessError(
                "No Default Material type set up in map."
                "Add a row to mapping file with *:s and a valid Material type"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.folio_material_types)} material types in FOLIO"
        )
