"""The Alabama mapper, responsible for parsing Items acording to the
FOLIO community specifications"""
import logging
from marc_to_folio.rules_mapper_base import RulesMapperBase
import uuid
import json
import csv
import sys
import traceback
from datetime import datetime
from folioclient import FolioClient
from typing import Set, Dict, List

csv.field_size_limit(sys.maxsize)


class ItemsDefaultMapper(RulesMapperBase):
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
        super().__init__(folio)
        self.args = args
        self.ref_data_dicts = {}
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
        self.loan_types = list(
            self.folio.folio_get_all("/loan-types", "loantypes"))
        print(f"{len(self.loan_types)} loan types set up in tenant", flush=True)
        self.material_types = list(
            self.folio.folio_get_all("/material-types", "mtypes")
        )
        print(f"{len(self.material_types)} material types set up in tenant", flush=True)
        """Locations stuff"""
        self.locations_map: Dict[str, str] = {}
        self.setup_locations(location_map)
        print(f"Location map set up with FOLIO locations", flush=True)
        """Note types"""
        self.item_note_types = list(
            self.folio.folio_get_all("/item-note-types", "itemNoteTypes")
        )
        print(
            f"{len(self.item_note_types)} Item note types set up in tenant", flush=True
        )
        self.note_id = next(
            x["id"] for x in self.item_note_types if "Note" == x["name"]
        )

        print(
            f"Default Loan type is {self.item_to_item_map['defaultLoantypeName']}",
            flush=True,
        )
        self.default_loan_type = self.get_ref_data_tuple_by_name(
            self.loan_types, "loan_types", self.item_to_item_map["defaultLoantypeName"]
        )
        print(
            f"Default Loan type UUID is {self.default_loan_type}", flush=True)

        print(
            f"Default Location code is {self.item_to_item_map['defaultLocationCode']}",
            flush=True,
        )
        self.default_location = self.locations_map.get(
            self.item_to_item_map["defaultLocationCode"],
        )
        print(f"Default Location UUID is {self.default_location}", flush=True)

        print(
            f"Default Material type code is {self.item_to_item_map['defaultMaterialTypeName']}",
            flush=True,
        )
        self.default_material_type = self.get_ref_data_tuple_by_name(
            self.material_types,
            "material_types",
            self.item_to_item_map["defaultMaterialTypeName"],
        )
        print(
            f"Default Material type UUID is {self.default_material_type}", flush=True)

    def parse_item(self, legacy_item: Dict):
        legacy_id = legacy_item[self.item_to_item_map["legacyIdField"]]
        fields_contents_to_report = self.item_to_item_map[
            "legacyFieldsToCountValuesFor"
        ]
        try:
            item = self.instantiate_item()
            for legacy_key, temp_legacy_value in legacy_item.items():
                legacy_value = (
                    str(temp_legacy_value).strip()
                    if str(temp_legacy_value).strip().lower() not in ["null", "none"]
                    else ""
                )
                legacy_key = legacy_key.strip() if legacy_key else legacy_key
                folio_field = (
                    self.item_to_item_map["fields"]
                    .get(legacy_key, {})
                    .get("target", "")
                )
                legacy_value = str(legacy_value).strip()
                if folio_field:
                    self.report_folio_mapping(
                        folio_field, True, not bool(legacy_value))
                    if legacy_key:
                        self.report_legacy_mapping(
                            legacy_key, True, True, not bool(legacy_value)
                        )
                elif legacy_key:
                    self.report_legacy_mapping(
                        legacy_key, True, False, not bool(legacy_value)
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
                        item[folio_field] = self.handle_material_types(
                            legacy_item)

                    elif folio_field == "status.name":
                        item["status"] = self.handle_status(legacy_value)
                    elif folio_field == "permanentLoanTypeId":
                        item[folio_field] = self.handle_loan_types(legacy_item)

                    elif folio_field == "itemLevelCallNumberTypeId":
                        item[folio_field] = self.handle_call_number_id(
                            legacy_value)

                    elif folio_field == "circulationNotes":
                        item[folio_field] = self.handle_circulation_notes(
                            legacy_value)
                    elif folio_field == "holdingsRecordId":
                        if legacy_value not in self.holdings_id_map:
                            self.add_stats(
                                self.stats, "Holdings id not in map")
                            # self.add_to_migration_report(
                            #    "Missing holdings ids", legacy_value
                            # )

                            raise ValueError(
                                f"Holdings id {legacy_value} not in map")
                        else:
                            item[folio_field] = self.holdings_id_map[legacy_value]["id"]

                    elif folio_field == "notes":
                        self.add_note(legacy_value, item)

                    else:
                        if self.is_string(folio_field):
                            item[folio_field] = str(
                                " ".join(
                                    [item.get(folio_field, ""), legacy_value])
                            ).strip()
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
            self.add_stats(self.stats, f"Total failed items with Value errors. Items not migrated")
            self.add_to_migration_report("ValueErrors", f"{ve}")
            return None
        except Exception as ee:
            self.add_stats(self.stats, "Exception. Items not migrated")
            self.add_to_migration_report("Exceptions", f"{ee} for {legacy_id}")
            print(f"{ee} for {legacy_id}")
            traceback.print_exc()
            raise ee

    def setup_locations(self, location_map):
        temp_map = {}
        for loc in self.folio.locations:
            key = loc[self.item_to_item_map["mapOnLocationField"]].strip()
            temp_map[key] = loc["id"]
        if location_map and any(location_map):
            for v in location_map:
                if "," in v["legacy_code"]:
                    for k in v["legacy_code"].split(","):
                        self.locations_map[k.strip(
                        )] = temp_map[v["folio_code"]]
                else:
                    temp_loc = temp_map.get(v["folio_code"], "")
                    if not temp_loc:
                        print((f'Folio location not found with code {v["folio_code"]}.'
                               'Adding {self.item_to_item_map["defaultLocationCode"]} instead'))
                        temp_loc = self.item_to_item_map["defaultLocationCode"]
                    self.locations_map[v["legacy_code"]] = temp_loc
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
        i = 0
        for s, v in sorted_hlids.items():
            i += 1
            self.add_to_migration_report(
                "Top missing holdings ids", f"{s} - {v}")
            if i > 15:
                break
        sorted_item_ids = {
            k: v
            for k, v in sorted(
                self.duplicate_item_ids.items(), key=lambda item: item[1], reverse=True,
            )
        }
        i = 0
        for s, v in sorted_item_ids.items():
            i += 1
            self.add_to_migration_report(
                "Top duplicate item ids", f"{s} - {v}")
            if i > 5:
                break
        # print("## Item transformation counters")
        # self.print_dict_to_md_table(self.stats)
        # self.write_migration_report()
        # self.print_mapping_report()

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

    def get_location_code(self, legacy_value: str):
        location_id = self.locations_map.get(
            legacy_value.strip().strip("^"), "")
        if location_id != "":
            return location_id
        else:
            self.add_to_migration_report(
                "Missing location codes", legacy_value)
            self.add_stats(
                self.stats, f'Missing location codes, adding "{self.default_location}"'
            )
            return self.default_location

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
            (x["id"]
             for x in self.item_note_types if note_type_name == x["name"]),
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

    def handle_circulation_notes(self, legacy_value):
        self.add_to_migration_report("Circulation notes", "Circ note")
        return []

    def handle_call_number_id(self, legacy_value):
        self.add_to_migration_report(
            "Call number legacy typesName - Not yet mapped", legacy_value
        )
        # return legacy_value
        return ""

    def handle_status(self, legacy_value):
        self.add_to_migration_report(
            "Legacy item status - Not mapped", legacy_value)
        # return legacy_value
        return {"name": "Available"}

    def handle_material_types(self, legacy_item: dict):
        m_keys = m_keys = list(
            [
                k.strip()
                for k in dict(self.legacy_material_type_map[0]).keys()
                if k != "folio_name"
            ]
        )
        fieldvalues = [legacy_item[k].strip() for k in m_keys]
        for row in self.legacy_material_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(
                    legacy_item[k].strip().casefold() in row[k].casefold())
            if all(all_good):
                folio_name = row["folio_name"]

                t = self.get_ref_data_tuple_by_name(
                    self.material_types, "material_types", folio_name
                )
                if t:
                    self.add_to_migration_report(
                        "Mapped Material Types", f'{t[1]} - {" - ".join(fieldvalues)}'
                    )
                    return t[0]
        self.add_to_migration_report(
            "Unapped Material Types", f'unspecified - {" - ".join(fieldvalues)}'
        )
        return self.default_material_type[0]

    def handle_loan_types(self, legacy_item: dict):
        m_keys = m_keys = list(
            [k for k in dict(self.legacy_loan_type_map[0]
                             ).keys() if k != "folio_name"]
        )
        fieldvalues = [legacy_item[k] for k in m_keys]
        for row in self.legacy_loan_type_map:
            all_good = []
            for k in m_keys:
                all_good.append(legacy_item[k] in row[k])
            if all(all_good):
                folio_name = row["folio_name"]
                t = self.get_ref_data_tuple_by_name(
                    self.loan_types, "loan_types", folio_name
                )
                if t:
                    self.add_to_migration_report(
                        "Mapped loan types", f'{t[1]}: {" - ".join(fieldvalues)}'
                    )
                    return t[0]

        self.add_to_migration_report(
            "Unmapped loan types",
            f'{self.default_loan_type} - {" - ".join(fieldvalues)}',
        )
        return self.default_loan_type

    def get_ref_data_tuple_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        if dict_key not in self.ref_data_dicts:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        ref_object = (
            self.ref_data_dicts[dict_key][key_value.lower()]
            if key_value.lower() in self.ref_data_dicts[dict_key]
            else None
        )
        if not ref_object:
            logging.debug(
                f"No matching element for {key_value} in {list(ref_data)}")
            return None
        return ref_object
