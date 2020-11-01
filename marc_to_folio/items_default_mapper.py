"""The Alabama mapper, responsible for parsing Items acording to the
FOLIO community specifications"""
from marc_to_folio.migration_helper import MigrationBase
import uuid
import json
import csv
import sys
import traceback
from datetime import datetime
from folioclient import FolioClient
from typing import Set, Dict, List

csv.field_size_limit(sys.maxsize)


class ItemsDefaultMapper(MigrationBase):
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
        args,
    ):
        super().__init__()
        self.args = args
        self.legacy_item_type_map = other_maps[0]
        self.duplicate_item_ids = {}
        self.legacy_material_type_map = other_maps[1]
        self.legacy_loan_type_map = other_maps[2]
        csv.register_dialect("tsv", delimiter="\t")
        csv.register_dialect("tsvq", delimiter="\t", quotechar='"')
        csv.register_dialect("pipe", delimiter="|")
        self.folio = folio
        self.missing_holdings_ids = {}
        self.item_schema = folio.get_item_schema()
        self.item_id_map: Dict[str, str] = {}
        self.item_to_item_map = item_map
        self.holdings_id_map = holdings_id_map
        self.loan_types = list(self.folio.folio_get_all("/loan-types", "loantypes"))
        print(f"{len(self.loan_types)} loan types set up in tenant")
        self.material_types = list(
            self.folio.folio_get_all("/material-types", "mtypes")
        )
        print(f"{len(self.material_types)} material types set up in tenant")
        """Locations stuff"""
        self.locations_map: Dict[str, str] = {}
        self.setup_locations(location_map)
        print(f"Location map set up with FOLIO locations")

        """Note types"""
        self.item_note_types = list(
            self.folio.folio_get_all("/item-note-types", "itemNoteTypes")
        )
        print(f"{len(self.item_note_types)} Item note types set up in tenant")
        self.note_id = next(
            x["id"] for x in self.item_note_types if "Note" == x["name"]
        )

    def setup_locations(self, location_map):
        temp_map = {}
        for loc in self.folio.locations:
            key = loc[self.item_to_item_map["mapOnLocationField"]].strip()
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
            if i > 5:
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
            if i > 5:
                break
        print("## Item transformation counters")
        self.print_dict_to_md_table(self.stats)
        self.write_migration_report()
        self.print_mapping_report()

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
            self.add_stats(self.stats, "Location code not found in FOLIO")
            raise ValueError(f"Location code not found in FOLIO: {loc_code}")
        return folio_loc_id

    def get_records(self, file):
        reader = None
        if self.item_to_item_map["itemsFileType"] == "TSV":
            reader = csv.DictReader(file, dialect="tsv")
        if self.item_to_item_map["itemsFileType"] == "PIPE":
            reader = csv.DictReader(file, dialect="pipe")
        if self.item_to_item_map["itemsFileType"] == "TSVQ":
            reader = csv.DictReader(file, dialect="tsvq")
        if self.item_to_item_map["itemsFileType"] == "CSV":
            csv.register_dialect(
                "my_csv", delimiter=self.item_to_item_map["itemsFileDelimiter"]
            )
            reader = csv.DictReader(file, dialect="my_csv")
        # reader = csv.DictReader(file, dialect="pipe")
        i = 0
        try:
            for row in reader:
                i += 1
                self.add_stats(self.stats, "Number of records in file(s)")
                # if i < 3:
                yield row
                # else:
                #    raise Exception("Stop!")
        except Exception as ee:
            print(f"row:{i}")
            print(ee)
            traceback.print_exc()
            raise ee

    def instantiate_item(self):
        item = {
            "id": str(uuid.uuid4()),
            "status": {"name": "Available"},
            "metadata": self.folio.get_metadata_construct(),
        }
        self.report_folio_mapping("id", True, False)
        self.report_folio_mapping("metaData", True, False)
        self.report_folio_mapping("status", True, False)
        return item

    def parse_item(self, legacy_item: Dict):
        legacy_id = legacy_item[self.item_to_item_map["legacyIdField"]]
        fields_contents_to_report = self.item_to_item_map[
            "legacyFieldsToCountValuesFor"
        ]
        try:
            item = self.instantiate_item()
            for legacy_key, legacy_value in legacy_item.items():
                legacy_key = legacy_key.strip() if legacy_key else legacy_key
                folio_field = (
                    self.item_to_item_map["fields"]
                    .get(legacy_key, {})
                    .get("target", "")
                )
                legacy_value = str(legacy_value).strip()
                if folio_field:
                    self.report_folio_mapping(folio_field, True, not bool(legacy_value))
                    if legacy_key:
                        self.report_legacy_mapping(
                            legacy_key, True, not bool(legacy_value)
                        )
                elif legacy_key:
                    self.report_legacy_mapping(
                        legacy_key, False, not bool(legacy_value)
                    )

                if folio_field and legacy_value:
                    if folio_field in ["permanentLocationId"]:
                        code = self.get_location_code(legacy_value)
                        if code:
                            item[folio_field] = code
                    elif folio_field in ["temporaryLocationId"]:
                        self.add_stats(
                            self.stats, f"Temp location code: {legacy_value}"
                        )
                        # TODO: set temporary location?
                    elif folio_field == "materialTypeId":
                        item[folio_field] = self.handle_material_types(legacy_item)

                    elif folio_field == "permanentLoanTypeId":
                        item[folio_field] = self.handle_loan_types(legacy_item)

                    elif folio_field == "holdingsRecordId":
                        if legacy_value not in self.holdings_id_map:
                            self.add_stats(self.stats, "Holdings id not in map")
                            # self.add_to_migration_report(
                            #    "Missing holdings ids", legacy_value
                            # )
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
                if legacy_key in fields_contents_to_report:
                    self.add_to_migration_report(
                        f"Field Contents - {legacy_key}", legacy_value
                    )
            has_failed: Set[str] = set()
            for f in self.item_schema["properties"]:
                if f not in item:
                    self.report_folio_mapping(f, False, False)

            for req in self.item_schema["required"]:
                if req not in item:
                    has_failed.add(req)
                    self.add_stats(
                        self.stats, f"Missing required field(s): {req}",
                    )
            if any(has_failed):
                raise ValueError(f"{list(has_failed)} is required")
            self.add_stats(self.stats, "Sucessfully transformed items")
            if not legacy_id.strip():
                self.add_stats(self.stats, "Empty legacy id")
            if legacy_id in self.item_id_map:
                self.add_stats(self.duplicate_item_ids, legacy_id)
                self.add_stats(self.stats, "Duplicate item ids")
            else:
                self.item_id_map[legacy_id] = item["id"]
            return item
        except ValueError as ve:
            self.add_stats(self.stats, f"Total failed items with Value errors")
            # self.add_to_migration_report("ValueErrors", f"{ve} for {legacy_id}")
            return None
        except Exception as ee:
            self.add_stats(self.stats, "Exception")
            self.add_to_migration_report("Exceptions", f"{ee} for {legacy_id}")
            print(f"{ee} for {legacy_id}")
            traceback.print_exc()
            raise ee

    def get_location_code(self, legacy_value: str):
        location_id = self.locations_map.get(legacy_value.strip().strip("^"), "")
        if location_id == "":
            self.add_to_migration_report("Missing location codes", legacy_value)
            self.add_stats(self.stats, 'Missing location codes, adding "tech"')
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

    def handle_material_types(self, legacy_item: dict):
        m_keys = m_keys = list(
            [
                k
                for k in dict(self.legacy_material_type_map[0]).keys()
                if k != "folio_name"
            ]
        )
        fieldvalues = [legacy_item[k].strip() for k in m_keys]
        folio_id = None
        for row in self.legacy_material_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(legacy_item[k].strip().casefold() in row[k].casefold())
            if all(all_good):
                folio_name = row["folio_name"]
                self.add_to_migration_report(
                    "Mapped Material Types", f'{folio_name} - {" - ".join(fieldvalues)}'
                )
                folio_id = next(
                    (
                        x["id"]
                        for x in self.material_types
                        if folio_name.casefold() == x["name"].casefold()
                    ),
                    "unspecified",
                )
                if folio_id != "unspecified":
                    return folio_id
        self.add_to_migration_report(
            "Unapped Material Types", f'unspecified - {" - ".join(fieldvalues)}'
        )
        return next(
            (
                x["id"]
                for x in self.material_types
                if x["name"].casefold() == "unspecified".casefold()
            )
        )

    def handle_loan_types(self, legacy_item: dict):
        m_keys = m_keys = list(
            [k for k in dict(self.legacy_loan_type_map[0]).keys() if k != "folio_name"]
        )
        fieldvalues = [legacy_item[k] for k in m_keys]
        folio_id = None
        for row in self.legacy_loan_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(legacy_item[k] in row[k])
            if all(all_good):
                folio_name = row["folio_name"]
                self.add_to_migration_report(
                    "Mapped loan types", f'{folio_name} - {" - ".join(fieldvalues)}'
                )

                folio_id = next(
                    (
                        x["id"]
                        for x in self.loan_types
                        if folio_name.casefold() == x["name"].casefold()
                    ),
                    "Non-Circulating",
                )
                if folio_id != "Non-Circulating":
                    return folio_id

        self.add_to_migration_report(
            "Unmapped loan types", f'Non-Circulating - {" - ".join(fieldvalues)}'
        )
        return next(
            (
                x["id"]
                for x in self.loan_types
                if x["name"].casefold() == "Non-Circulating".casefold()
            )
        )
