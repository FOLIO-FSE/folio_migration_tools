"""The Alabama mapper, responsible for parsing Items acording to the
FOLIO community specifications"""
import uuid
import json
import csv
import sys
import traceback
from datetime import datetime
from folioclient import FolioClient
from typing import Set, Dict, List

csv.field_size_limit(sys.maxsize)


class ItemsDefaultMapper:
    """Maps an Item to inventory Item format according to
    the FOLIO community convention"""

    # Bootstrapping (loads data needed later in the script.)

    def __init__(
        self,
        folio: FolioClient,
        item_map: Dict,
        holdings_id_map: Dict,
        location_map: Dict,
        other_maps,
    ):
        self.legacy_item_type_map = other_maps[0]
        self.duplicate_item_ids = {}
        self.legacy_material_type_map = other_maps[1]
        self.legacy_loan_type_map = other_maps[2]
        csv.register_dialect("tsv", delimiter="\t")
        csv.register_dialect("tsvq", delimiter="\t", quotechar='"')
        csv.register_dialect("pipe", delimiter="|")
        self.folio = folio
        self.stats: Dict[str, int] = {}
        self.missing_holdings_ids: Dict[str, int] = {}
        self.migration_report: Dict[str, List[str]] = {}
        self.item_schema = folio.get_item_schema()
        self.item_id_map: Dict[str, str] = {}
        self.map = item_map
        self.holdings_id_map = holdings_id_map
        self.mapped_folio_fields: Dict[str, int] = {}
        self.mapped_legacy_field: Dict[str, int] = {}
        self.unmapped_legacy_field: Dict[str, int] = {}

        """Locations stuff"""
        self.locations_map: Dict[str, str] = {}
        self.missing_location_codes: Dict[str, int] = {}
        self.setup_locations(location_map)
        self.unmapped_location_items: Dict[str, int] = {}

        """Loan types, material types"""
        self.loan_type_map: Dict[str, int] = {}
        self.material_type_map: Dict[str, int] = {}
        self.unmapped_loan_types: Dict[str, int] = {}
        self.unmapped_material_types: Dict[str, int] = {}
        self.setup_l_types()
        self.setup_m_types()

        """Note types"""
        self.item_note_types = self.folio.folio_get_all(
            "/item-note-types", "itemNoteTypes"
        )
        self.note_id = next(
            x["id"] for x in self.item_note_types if "Note" == x["name"]
        )

    def setup_m_types(self,):
        material_types = self.folio.folio_get_all("/material-types", "mtypes")
        if self.legacy_item_type_map:
            for b in self.legacy_item_type_map:
                mt_id = next(
                    (
                        x["id"]
                        for x in material_types
                        if b["material_type"].casefold() == x["name"].casefold()
                    ),
                    "Unmapped",
                )
                if mt_id == "Unmapped":
                    add_stats(
                        self.stats, f'Unmapped material type code {b["legacy_type"]}'
                    )
                else:
                    self.material_type_map[b["legacy_type"]] = mt_id
            print(f"set up Material Type Map: {len(self.material_type_map)} rows.")
        elif self.legacy_material_type_map:
            for b in self.legacy_material_type_map:
                mt_id = next(
                    (
                        x["id"]
                        for x in material_types
                        if b["folio_name"].casefold() == x["name"].casefold()
                    ),
                    "Unmapped",
                )
                if mt_id == "Unmapped":
                    add_stats(
                        self.stats, f'Unmapped material type code {b["legacy_code"]}'
                    )
                else:
                    self.material_type_map[b["legacy_code"]] = mt_id
            print(f"set up Material Type Map: {len(self.material_type_map)} rows.")
        else:
            raise Exception("No map for Material types supplied")

    def setup_l_types(self,):
        loan_types = self.folio.folio_get_all("/loan-types", "loantypes")
        if self.legacy_item_type_map:
            for b in self.legacy_item_type_map:
                lt_id = next(
                    (
                        x["id"]
                        for x in loan_types
                        if b["loan_type"].casefold() == x["name"].casefold()
                    ),
                    "Unmapped",
                )

                if lt_id == "Unmapped":
                    add_stats(self.stats, f'Unmapped loan type code {b["legacy_type"]}')
                else:
                    self.loan_type_map[b["legacy_type"]] = lt_id
            print(f"set up LoanTypeMap: {len(self.loan_type_map)} rows.")
        elif self.legacy_loan_type_map:
            for b in self.legacy_loan_type_map:
                lt_id = next(
                    (
                        x["id"]
                        for x in loan_types
                        if b["folio_name"].casefold() == x["name"].casefold()
                    ),
                    "Unmapped",
                )
                self.loan_type_map[b["legacy_code"]] = lt_id
                if lt_id == "Unmapped":
                    add_stats(
                        self.stats, f'Unmapped material type code {b["legacy_code"]}'
                    )
                else:
                    self.loan_type_map[b["legacy_code"]] = lt_id
            print(f"set up Loan Type Map: {len(self.loan_type_map)} rows.")

        else:
            raise Exception("No map for Loan types supplied")

    def setup_locations(self, location_map):
        temp_map = {}
        for loc in self.folio.locations:
            key = loc[self.map["mapOnLocationField"]].strip()
            temp_map[key] = loc["id"]
        if location_map and any(location_map):
            for v in location_map:
                if "," in v["legacy_code"]:
                    for k in v["legacy_code"].split(","):
                        self.locations_map[k.strip()] = temp_map[v["folio_code"]]
                else:
                    self.locations_map[v["legacy_code"]] = temp_map[v["folio_code"]]
        else:
            print("No location map supplied")
            self.locations_map = temp_map
        # self.locations_map = location_map
        # print(json.dumps(self.locations_map, indent=4))

    def wrap_up(self):
        sorted_hlids = {
            k: v
            for k, v in sorted(
                self.missing_holdings_ids.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        }
        print("## Top missing holdings ids ")
        i = 0
        for s, v in sorted_hlids.items():
            i += 1
            print(f"{s} - {v}")
            if i > 10:
                break
        sorted_item_ids = {
            k: v
            for k, v in sorted(
                self.duplicate_item_ids.items(), key=lambda item: item[1], reverse=True,
            )
        }
        i = 0
        print("## Top duplicate item ids")
        for s, v in sorted_item_ids.items():
            i += 1
            print(f"{s} - {v}")
            if i > 10:
                break
        print("## Item transformation counters")
        print_dict_to_md_table(self.stats)
        print("## Unmapped location items")
        print_dict_to_md_table(self.unmapped_location_items)
        print("## Mapped FOLIO fields")
        print_dict_to_md_table(self.mapped_folio_fields)
        print("## Mapped legacy fields")
        print_dict_to_md_table(self.mapped_legacy_field)
        print("## Unmapped legacy fields")
        print_dict_to_md_table(self.unmapped_legacy_field)
        print("## Missing location codes")
        print_dict_to_md_table(self.missing_location_codes)
        print("## Unmapped loan types")
        print_dict_to_md_table(self.unmapped_loan_types)
        print("## Unmapped material types")
        print_dict_to_md_table(self.unmapped_material_types)
        # self.write_migration_report()

    def write_migration_report(self, other_report=None):
        if other_report:
            for a in other_report:
                print(f"## {a} - {len(other_report[a])} things")
                for b in other_report[a]:
                    print(f"{b}\\")
        else:
            for a in self.migration_report:
                print(f"## {a} - {len(self.migration_report[a])} things")
                for b in self.migration_report[a]:
                    print(f"{b}\\")

    def get_loc_id(self, loc_code):
        folio_loc_id = next(
            (
                l["id"]
                for l in self.folio.locations
                if loc_code.strip().casefold() == l["code"].casefold()
            ),
            "",
        )
        if not folio_loc_id:
            add_stats(self.stats, "Location code not found in FOLIO")
            raise ValueError(f"Location code not found in FOLIO: {loc_code}")
        return folio_loc_id

    def get_records(self, file):
        reader = None
        if self.map["itemsFileType"] == "TSV":
            reader = csv.DictReader(file, dialect="tsv")
        if self.map["itemsFileType"] == "PIPE":
            reader = csv.DictReader(file, dialect="pipe")
        if self.map["itemsFileType"] == "TSVQ":
            reader = csv.DictReader(file, dialect="tsvq")
        if self.map["itemsFileType"] == "CSV":
            csv.register_dialect("my_csv", delimiter=self.map["itemsFileDelimiter"])
            reader = csv.DictReader(file, dialect="my_csv")
        # reader = csv.DictReader(file, dialect="pipe")
        i = 0
        try:
            for row in reader:
                i += 1
                add_stats(self.stats, "Number of records in file(s)")
                yield row
        except Exception as ee:
            print(f"row:{i}")
            print(ee)
            traceback.print_exc()
            raise ee

    def parse_item(self, legacy_item: Dict):
        legacy_id = legacy_item[self.map["legacyIdField"]]
        try:
            item = {
                "id": str(uuid.uuid4()),
                "status": {"name": "Available"},
                "metadata": self.folio.get_metadata_construct(),
            }
            add_stats(self.mapped_folio_fields, "id")
            add_stats(self.mapped_folio_fields, "metadata")
            add_stats(self.mapped_folio_fields, "status")
            for legacy_key, legacy_value in legacy_item.items():
                folio_field = self.map["fields"].get(legacy_key, {}).get("target", "")
                legacy_value = str(legacy_value).strip()
                if folio_field:
                    add_stats(self.mapped_folio_fields, folio_field)
                    add_stats(self.mapped_legacy_field, legacy_key or "")
                elif legacy_value:
                    add_stats(self.unmapped_legacy_field, legacy_key or "")
                if folio_field and legacy_value.strip():
                    if folio_field in ["permanentLocationId"]:
                        code = self.get_location_code(legacy_value)
                        if code:
                            item[folio_field] = code
                    elif folio_field in ["temporaryLocationId"]:
                        add_stats(self.stats, f"Temp location code: {legacy_value}")
                    elif folio_field == "materialTypeId":
                        self.handle_material_types(legacy_value, item)
                        # if "loanTypeId" not in self.map["fields"]:
                        #    self.handle_loan_types(legacy_value, item)

                    elif folio_field == "loanTypeId":
                        self.handle_loan_types(legacy_value, item)

                    elif folio_field == "holdingsRecordId":
                        if legacy_value not in self.holdings_id_map:
                            add_stats(self.stats, "Holdings id not in map")
                            add_stats(self.missing_holdings_ids, legacy_value)
                            raise ValueError(f"Holdings id {legacy_value} not in map")
                        else:
                            item[folio_field] = self.holdings_id_map[legacy_value]

                    elif folio_field == "notes":
                        self.add_note(legacy_value, item)

                    else:
                        if self.is_string(folio_field):
                            item[folio_field] = legacy_value
                        elif self.is_string_array(folio_field):
                            if folio_field in item and any(item[folio_field]):
                                item[folio_field].append(legacy_value)
                            else:
                                item[folio_field] = [legacy_value]
            has_failed: Set[str] = set()
            for req in self.item_schema["required"]:
                if req not in item:
                    has_failed.add(req)
                    add_stats(
                        self.stats, f"Missing required field(s): {req}",
                    )
            if any(has_failed):
                raise ValueError(f"{list(has_failed)} is required")
            add_stats(self.stats, "Sucessfully transformed items")
            if not legacy_id.strip():
                add_stats(self.stats, "Empty legacy id")
            if legacy_id in self.item_id_map:
                add_stats(self.duplicate_item_ids, legacy_id)
                add_stats(self.stats, "Duplicate item ids")
            else:
                self.item_id_map[legacy_id] = item["id"]
            return item
        except ValueError as ve:
            # add_stats(self.stats, f"Failed items with Value errors {ve}")
            add_stats(self.stats, f"Total failed items with Value errors")
            self.add_to_migration_report("ValueErrors", f"{ve} for {legacy_id}")
            return None
        except Exception as ee:
            add_stats(self.stats, "Exception")
            self.add_to_migration_report("Exceptions", f"{ee} for {legacy_id}")
            print(f"{ee} for {legacy_id}")
            traceback.print_exc()
            raise ee

    def get_location_code(self, legacy_value: str):
        location_id = self.locations_map.get(legacy_value.strip().strip("^"), "")
        if location_id == "":
            add_stats(self.missing_location_codes, legacy_value)
            add_stats(self.stats, 'missing_location_codes, adding "tech"')
            location_id = self.locations_map.get("tech", "")
        return location_id

    def is_string(self, target: str):
        folio_prop = self.item_schema["properties"][target]["type"]
        return folio_prop == "string"

    def is_string_array(self, target: str):
        f = self.item_schema["properties"][target]
        return f["type"] == "array" and f["items"]["type"] == "string"

    def add_note(
        self,
        note_string: str,
        item: Dict,
        note_type_name: str = "",
        staffOnly: bool = False,
    ):
        nt_id = next(
            (x["id"] for x in self.item_note_types if note_type_name == x["name"]),
            self.note_id,
        )
        note_to_add = {
            "itemNoteTypeId": nt_id,
            "note": note_string,
            "staffOnly": staffOnly,
        }
        if "notes" in item and any(item["notes"]):
            item["notes"].append(note_to_add)
        else:
            item["notes"] = [note_to_add]

    def handle_material_types(self, legacy_value: str, item: Dict):
        v = legacy_value
        if v.startswith("DigitalEquip"):
            v = "DigitalEquipment"
            # TODO: map value!
            cost = v.replace("DigitalEquip_", "")
            self.add_note(cost, item, "Replacement Cost (USD)", True)
        elif v not in self.material_type_map:
            if "*" in self.material_type_map:
                mapped_id = self.material_type_map["*"]
                item["materialTypeId"] = mapped_id
                add_stats(self.stats, "* material types")
            else:
                add_stats(self.unmapped_material_types, v)
                add_stats(self.stats, "Unmapped material types")
        else:
            mapped_id = self.material_type_map[v]
            item["materialTypeId"] = mapped_id

    def handle_loan_types(self, legacy_value, item):
        v = legacy_value
        if v.startswith("DigitalEquip"):
            v = "DigitalEquipment"
            # TODO: map value!
            cost = v.replace("DigitalEquip_", "")
            self.add_note(cost, item, "Replacement Cost (USD)", True)
            add_stats(self.stats, "Cost notes added")
        if v not in self.loan_type_map:
            if "*" in self.loan_type_map:
                mapped_id = self.loan_type_map["*"]
                item["permanentLoanTypeId"] = mapped_id
                add_stats(self.stats, "Items that got assigned * loan types")
            else:
                self.unmapped_loan_types[v] = self.unmapped_loan_types.get(v, 0) + 1
                add_stats(self.stats, f"Unmapped item loan type")
        else:
            item["permanentLoanTypeId"] = self.loan_type_map[v]

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def print_dict_to_md_table(my_dict, h1="Measure", h2="Number"):
    # TODO: Move to interface or parent class
    d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
    print(f"{h1} | {h2}")
    print("--- | ---:")
    for k, v in d_sorted.items():
        print(f"{k} | {v:,}")
