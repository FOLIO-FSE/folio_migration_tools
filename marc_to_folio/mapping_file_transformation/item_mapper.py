import csv
import json
from marc_to_folio.custom_exceptions import (
    TransformationCriticalDataError,
    TransformationProcessError,
)
from typing import List
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
import re
import uuid
from datetime import datetime

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
        holdings_id_map,
        error_file,
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(folio_client, item_schema, items_map, error_file)
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map

        self.loan_type_map = loan_type_map
        self.legacy_id_map: Dict[str, str] = {}
        self.ids_dict: Dict[str, set] = {}
        self.use_map = True

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
        if self.use_map:
            legacy_item_keys = list(
                k["legacy_field"]
                for k in self.items_map["data"]
                if k["folio_field"] == folio_prop_name
            )
            vals = list([v for k, v in legacy_item.items() if k in legacy_item_keys])
            legacy_value = " ".join(vals).strip()
            self.add_to_migration_report("Source fields with same target", len(vals))
            # legacy_value = legacy_item.get(legacy_item_key, "")
            if folio_prop_name in ["permanentLocationId", "temporaryLocationId"]:
                return self.get_location_id(legacy_item, index_or_id)
            elif folio_prop_name == "materialTypeId":
                return self.get_material_type_id(legacy_item)
            elif folio_prop_name == "status.name":
                return self.transform_status(legacy_value)
            elif folio_prop_name == "permanentLoanTypeId":
                return self.get_loan_type_id(legacy_item)
            elif folio_prop_name == "statisticalCodeIds":
                return self.get_statistical_codes(vals)
            elif folio_prop_name == "holdingsRecordId":
                if legacy_value not in self.holdings_id_map:
                    self.add_stats("Holdings id not in map")
                    raise TransformationProcessError(
                        index_or_id, f"Holdings id '{legacy_value}' not in list of mapped holdings."
                    )
                else:
                    self.add_to_migration_report("Holdings IDs", f"Mapped")
                    return self.holdings_id_map[legacy_value]["id"]
            elif len(legacy_item_keys) == 1:
                # print(folio_prop_name)
                value = next((k.get("value","") for k in self.items_map["data"]
                        if k["folio_field"] == folio_prop_name),"")
                if value:
                    return value
                else:
                    return legacy_value
            elif any(legacy_item_keys):
                return legacy_value
            else:
                # self.report_folio_mapping(f"{folio_prop_name}", False, False)
                return ""
        else:
            self.report_folio_mapping(f"{folio_prop_name}", True, False)
            return legacy_item[folio_prop_name]

    def get_note(
        self,
        note_string: str,
        note_type_name: str = "",
        staffOnly: bool = False,
    ):
        nt_id = next(
            (x["id"] for x in self.item_note_types if note_type_name == x["name"]),
            self.note_id,
        )
        return {
            "itemNoteTypeId": nt_id,
            "note": note_string,
            "staffOnly": staffOnly,
        }

    def get_circulation_notes(self, legacy_value):
        self.add_to_migration_report("Circulation notes", "Circ note")
        return []

    def get_statistical_codes(self, vals) -> List(str):
        # Mapping file with old values to FOLIO equivalents and then map that here.
        raise NotImplementedError("Statistical code mapping is not yet available")
    
    def get_loan_type_id(self, legacy_item: dict):
        fieldvalues = [legacy_item[k] for k in self.loan_type_keys]
        for row in self.loan_type_map:
            all_good = []
            for k in self.loan_type_keys:
                all_good.append(legacy_item[k] in row[k])
            if all(all_good):
                self.add_to_migration_report(
                    "Mapped loan types",
                    f'{" - ".join(fieldvalues)} -> {row["folio_name"]}',
                )
                return row["folio_id"]

        self.add_to_migration_report(
            "Unmapped loan types",
            f'{" - ".join(fieldvalues)}',
        )
        return self.default_loan_type_id

    def get_material_type_id(self, legacy_item: dict):
        for row in self.material_type_map:
            all_good = []
            for k in self.material_type_keys:
                all_good.append(legacy_item[k].strip().casefold() in row[k].casefold())
            fieldvalues = [legacy_item[k] for k in self.material_type_keys]
            if all(all_good):
                self.add_to_migration_report(
                    "Material type mapping",
                    f'{" - ".join(fieldvalues)} -> {row["folio_name"]}',
                )
                return row["folio_id"]
        self.add_to_migration_report(
            "Unmapped material types",
            f'{" - ".join(fieldvalues)}',
        )
        return self.default_material_type_id

    def get_location_id(self, legacy_item: dict, id_or_index):
        fieldvalues = [legacy_item[k] for k in self.location_keys]
        for row in self.location_map:
            all_good = []
            for k in self.location_keys:
                all_good.append(legacy_item[k] in row[k])
            if all(all_good):
                self.add_to_migration_report(
                    "Mapped locations",
                    f'{" - ".join(fieldvalues)} -> {row["folio_code"]}',
                )
                return row["folio_id"]
        self.add_to_migration_report(
            "Unmapped locations",
            f'{" - ".join(fieldvalues)}',
        )
        return self.default_location_id

    def transform_status(self, legacy_value):
        self.add_to_migration_report("Status mapping", f"{legacy_value} -> Available")
        return {"name": "Available"}

    def setup_loan_type_mappings(self):
        # Loan types
        print("Fetching Loan types...")
        self.folio_loan_types = list(
            self.folio_client.folio_get_all("/loan-types", "loantypes")
        )
        for idx, loan_type_mapping in enumerate(self.loan_type_map):
            try:
                if idx == 1:
                    self.loan_type_keys = list(
                        [
                            k
                            for k in loan_type_mapping.keys()
                            if k not in ["folio_code", "folio_id", "folio_name"]
                        ]
                    )
                if any(m for m in loan_type_mapping.values() if m == "*"):
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_loan_types,
                        "loan_types",
                        loan_type_mapping["folio_name"],
                    )
                    if t:
                        self.default_loan_type_id = t[0]
                        print(
                            f'Set {loan_type_mapping["folio_name"]} as default Loantype mapping'
                        )
                    else:
                        raise TransformationProcessError(
                            "No Default Loan type set up in map."
                            "Add a row to mapping file with *:s and a valid loan type"
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
                print(json.dumps(self.loan_type_map, indent=4))
                raise TransformationProcessError(
                    f"{loan_type_mapping['folio_name']} could not be found in FOLIO"
                )
        if not self.default_loan_type_id:
            raise TransformationProcessError(
                "No Default Loan type set up in map."
                "Add a row to mapping file with *:s and a valid loan type"
            )
        print(
            f"loaded {idx} mappings for {len(self.folio_loan_types)} loan types in FOLIO"
        )

    def setup_material_type_mappings(self):
        # Material types
        print("Fetching Material types...")
        self.folio_material_types = list(
            self.folio_client.folio_get_all("/material-types", "mtypes")
        )
        for idx, mat_mapping in enumerate(self.material_type_map):
            try:
                if idx == 1:
                    self.material_type_keys = list(
                        [
                            k
                            for k in mat_mapping.keys()
                            if k not in ["folio_code", "folio_id", "folio_name"]
                        ]
                    )
                if any(m for m in mat_mapping.values() if m == "*"):
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_material_types,
                        "mat_types",
                        mat_mapping["folio_name"],
                    )
                    if t:
                        self.default_material_type_id = t[0]
                        print(
                            f'Set {mat_mapping["folio_name"]} as default material type mapping'
                        )
                    else:
                        raise TransformationProcessError(
                            "No Default Material type set up in map."
                            "Add a row to mapping file with *:s and a valid Material type"
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
        print(
            f"loaded {idx} mappings for {len(self.folio_material_types)} material types in FOLIO"
        )

    def setup_location_mappings(self, location_map):
        # Locations
        print("Fetching locations...")
        for idx, loc_map in enumerate(location_map):
            if idx == 1:
                    self.location_keys = list(
                        [
                            k
                            for k in loc_map.keys()
                            if k not in ["folio_code", "folio_id", "folio_name"]
                        ]
                    )
            if any(m for m in loc_map.values() if m == "*"):
                t = self.get_ref_data_tuple_by_code(
                    self.folio_client.locations, "locations", loc_map["folio_code"]
                )
                if t:
                    self.default_location_id = t[0]
                    print(f'Set {loc_map["folio_code"]} as default location')
                else:
                    raise TransformationProcessError(
                        f"Default location {loc_map['folio_code']} not found in folio. "
                        "Change default code"
                    )
            else:
                t = self.get_ref_data_tuple_by_code(
                    self.folio_client.locations, "locations", loc_map["folio_code"]
                )
                if t:
                    loc_map["folio_id"] = t[0]
                else:
                    raise Exception(
                        f"Location code {loc_map['folio_code']} from map not found in FOLIO"
                    )

        if not self.default_location_id:
            raise TransformationProcessError(
                "No Default Location set up in map. "
                "Add a row to mapping file with *:s and a valid Location code"
            )
        print(
            f"loaded {idx} mappings for {len(self.folio_client.locations)} locations in FOLIO"
        )
