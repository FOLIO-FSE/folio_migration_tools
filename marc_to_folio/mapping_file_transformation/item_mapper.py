import csv
import json
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
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(folio_client, item_schema, items_map)
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map
        
        self.loan_type_map = loan_type_map
        self.legacy_id_map: Dict[str, str] = {}
        self.ids_dict: Dict[str, set] = {}
        self.use_map = True
        
        # Loan types
        self.loan_type_map = loan_type_map
        print("Fetching Loan types...")
        self.folio_loan_types = list(
            self.folio_client.folio_get_all("/loan-types", "loantypes"))
        for idx, loan_type_mapping in enumerate(self.loan_type_map):
            try:
                loan_type_mapping["folio_id"] = self.get_ref_data_tuple_by_name(self.folio_loan_types, "loan_types", loan_type_mapping["folio_name"])[0]
            except:
                print(json.dumps(self.loan_type_map, indent=4))
                raise Exception(f"{loan_type_mapping['folio_name']} could not be found in FOLIO")
        print(
            f"loaded {idx} mappings for {len(self.folio_loan_types)} loan types in FOLIO"
        )
        
        # Material types
        self.material_type_map = material_type_map
        print("Fetching Material types...")
        self.folio_material_types = list(
            self.folio_client.folio_get_all("/material-types", "mtypes")
        )
        for idx, mat_mapping in enumerate(self.material_type_map):
            try:
                mat_mapping["folio_id"] = self.get_ref_data_tuple_by_name(self.folio_material_types, "mat_types", mat_mapping["folio_name"])[0]
            except:
                raise Exception(f"{mat_mapping['folio_name']} could not be found in FOLIO")
        print(
            f"loaded {idx} mappings for {len(self.folio_material_types)} material types in FOLIO"
        )

        # Locations
        print("Fetching locations...")
        self.location_map = {}
        for idx, loc_map in enumerate(location_map):
            try:
                self.location_map[
                    loc_map["legacy_code"]
                ] = self.get_ref_data_tuple_by_code(
                    self.folio_client.locations, "locations", loc_map["folio_code"]
                )[
                    0
                ]
            except:
                raise Exception(
                    f"Location code {loc_map['legacy_code']} - {loc_map['legacy_code']} from map not found in FOLIO"
                )
        print(
            f"loaded {idx} mappings for {len(self.folio_client.locations)} locations in FOLIO"
        )

        # Defaults
        self.default_location_uuid = str(uuid.uuid4())

    def perform_additional_mappings(self):
        raise NotImplementedError()

    def get_prop(self, legacy_item, folio_prop_name, i=0):
        if self.use_map:
            legacy_item_keys = list(
                k["legacy_field"]
                for k in self.items_map["data"]
                if k["folio_field"].replace(f"[{i}]", "") == folio_prop_name
            )
            vals = list([v for k, v in legacy_item.items() if k in legacy_item_keys])
            self.add_to_migration_report("Concatenated fields", len(vals))
            legacy_value = " ".join(vals).strip()
            # legacy_value = legacy_item.get(legacy_item_key, "")
            if folio_prop_name in ["permanentLocationId", "temporaryLocationId"]:
                return self.get_location_id(legacy_value)
            elif folio_prop_name == "materialTypeId":
                return self.get_material_type_id(legacy_item)
            elif folio_prop_name == "status.name":
                return self.transform_status(legacy_value)
            elif folio_prop_name == "permanentLoanTypeId":
                return self.get_loan_type_id(legacy_item)
            elif folio_prop_name == "itemLevelCallNumberTypeId":
                return self.get_call_number_id(legacy_value)
            elif folio_prop_name == "statisticalCodeIds":
                (code_id, note) = self.handle_statistical_codes(legacy_value)
                if code_id:
                    return code_id
            elif folio_prop_name == "circulationNotes":
                return self.get_circulation_notes(legacy_value)
            elif folio_prop_name == "holdingsRecordId":
                if legacy_value not in self.holdings_id_map:
                    self.add_stats("Holdings id not in map")
                    self.add_to_migration_report(
                        "Holdings IDs",
                        f"Unable to find Holdings id in map for {legacy_value}",
                    )
                    raise ValueError(f"Holdings id '{legacy_value}' not in map")
                else:
                    self.add_to_migration_report("Holdings IDs", f"Mapped")
                    return self.holdings_id_map[legacy_value]["id"]
            elif folio_prop_name == "notes":
                return self.get_note(legacy_value)
            elif any(legacy_item_keys):
                return legacy_value
            else:
                # self.report_folio_mapping(f"{folio_prop_name}", False, False)
                return ""
        else:
            self.report_folio_mapping(f"{folio_prop_name}", True, False)
            return legacy_item[folio_prop_name]

    def get_call_number_id(self, legacy_value):
        self.add_to_migration_report(
            "Call number legacy typesName - Not yet mapped", legacy_value
        )
        # return legacy_value
        return ""

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

    def get_loan_type_id(self, legacy_item: dict):
        m_keys = m_keys = list(
            [k for k in dict(self.loan_type_map[0]).keys() if k not in ["folio_name", "folio_id"]]
        )
        fieldvalues = [legacy_item[k] for k in m_keys]
        for row in self.loan_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(legacy_item[k] in row[k])
            if all(all_good):
                self.add_to_migration_report(
                        "Mapped loan types", f'{" - ".join(fieldvalues)} -> {row["folio_name"]}'
                    )
                return row["folio_id"]

        self.add_to_migration_report(
            "Unmapped loan types",
            f'{" - ".join(fieldvalues)}',
        )
        return ""

    def get_location_id(self, legacy_value: str):
        location_id = self.location_map.get(legacy_value.strip().strip("^"), "")
        if location_id != "":
            self.add_to_migration_report("Location Mapping", f"{legacy_value} -> {location_id}")
            return location_id
        else:
            self.add_to_migration_report("Location Mapping", f"Unmapped legacy code {legacy_value}")
            self.add_stats(
                f'Missing location codes, adding "{self.default_location_uuid}"',
            )
            return self.default_location_uuid

    def transform_status(self, legacy_value):
        self.add_to_migration_report("Status mapping", f"{legacy_value} -> Available")
        return {"name": "Available"}

    def get_material_type_id(self, legacy_item: dict):
        m_keys = m_keys = list(
            [
                k.strip()
                for k in dict(self.material_type_map[0]).keys()
                if k not in ["folio_name", "folio_id"]
            ]
        )
        for row in self.material_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(legacy_item[k].strip().casefold() in row[k].casefold())
            fieldvalues = [legacy_item[k] for k in m_keys]
            if all(all_good):
                self.add_to_migration_report("Material type mapping", f'{" - ".join(fieldvalues)} -> {row["folio_name"]}')
                return row["folio_id"]
        self.add_to_migration_report(
            "Unmapped material types",
            f'{" - ".join(fieldvalues)}',
        )
        return "" # self.default_material_type[0]
