"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import uuid
import requests
import logging
import json
import pymarc
from datetime import datetime


class HoldingsDefaultMapper:
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    # Bootstrapping (loads data needed later in the script.)

    def __init__(self, folio_client, instance_id_map, location_map=None):

        print("init Default Holdings mapper!")
        self.stats = {}
        self.tags_occurrences = {}
        self.location_map = location_map
        self.migration_report = {}
        self.folio_client = folio_client
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}
        self.call_number_types_2 = {}
        self.call_number_types_ind1 = {}
        self.unmapped_holdings_fields = {}
        self.mapped_folio_fields = {}
        self.call_number_types_ind2 = {}
        self.f852as = {}
        self.folio_locations = {}
        self.legacy_locations = {}
        self.unmapped_locations = {}
        self.holdings_schema = fetch_holdings_schema()
        self.holdings_note_types = self.folio_client.folio_get_all(
            "/holdings-note-types", "holdingsNoteTypes"
        )
        print(f"{len(self.holdings_note_types)} note types")
        self.call_number_types = self.folio_client.folio_get_all(
            "/call-number-types", "callNumberTypes"
        )
        # Send out a list of note types that should be either public or private
        self.note_tags = {
            "506": ("3abcdefgqu", "Restriction", False, " "),
            "538": ("aiu3568", "Note", False, " "),
            "541": ("3abcdefhno568", "Note", False, " "),
            "561": ("3au", "Provenance", False, " "),
            "562": ("3abcde", "Copy note", False, " "),
            "563": ("3au", "Binding", False, " "),  # Binding note type
            "583": (
                "3abcdefhijklnouxz",
                "Action note",
                False,
                " ",
            ),  # Map to Action note
            "843": ("3abcdefmn", "Reproduction", False, " "),
            "845": ("abcdu3568", "Note", False, " "),
            "852": ("x", "Note", False, " "),
            "852": ("z", "Note", False, " "),
            "876": ("p3", "bound with item data", False, " | "),
        }

    def parse_hold(self, marc_record):
        """Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: """
        for f in marc_record:
            add_stats(self.tags_occurrences, f.tag)

        f852s = self.report_on_852s(marc_record)
        rec = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio_client.get_metadata_construct(),
            "hrid": marc_record["001"].format_field(),
            "instanceId": self.get_instance_id(marc_record),
            "holdingsStatements": list(self.get_holdingsStatements(marc_record, "866")),
            "notes": list(self.get_notes(marc_record)),
            "holdingsStatementsForSupplements": list(
                self.get_holdingsStatements(marc_record, "867")
            ),
            "holdingsStatementsForIndexes": list(
                self.get_holdingsStatements(marc_record, "868")
            ),
        }
        rec.update(self.handle_852s(f852s))
        self.holdings_id_map[marc_record["001"].format_field()] = rec["id"]
        count_unmapped_fields(
            self.unmapped_holdings_fields, self.holdings_schema, rec,
        )
        count_mapped_fields(self.mapped_folio_fields, rec)
        self.validate(rec, marc_record)
        return rec

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.holdings_id_map:
            del self.holdings_id_map[id_key]

    def report_on_852s(self, marc_record):
        if "852" not in marc_record:
            add_stats(self.stats, "Records missing 852")
            raise ValueError(f'852 missing for {marc_record["001"]}')
        f852s = marc_record.get_fields("852")
        if len(f852s) > 1:
            add_stats(self.stats, "Records with multiple 852 fields")
            for f852 in f852s:
                for sf in f852.get_subfields():
                    add_stats(self.stats, f"852 - {sf}")
        return f852s

    def wrap_up(self):
        print("## Holdings transformation stats")
        print_dict_to_md_table(self.stats)
        print("## Legacy locations")
        print_dict_to_md_table(self.legacy_locations)
        print("## FOLIO locations")
        print_dict_to_md_table(self.folio_locations)
        print("## Unmapped location codes")
        print_dict_to_md_table(self.unmapped_locations)
        print("## Call number types (852 $2) values")
        print_dict_to_md_table(self.call_number_types_2)
        print("## Unmapped FOLIO fields")
        print_dict_to_md_table(self.unmapped_holdings_fields)
        print("## Mapped FOLIO Fields")
        print_dict_to_md_table(self.mapped_folio_fields)
        self.write_migration_report()

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

    def write_migration_report(self, other_report=None):
        if other_report:
            for a in other_report:
                print(f"## {a}")
                for b in other_report[a]:
                    print(f"{b}\\")
        else:
            for a in self.migration_report:
                print(f"## {a}")
                for b in self.migration_report[a]:
                    print(f"{b}\\")

    def get_instance_id(self, marc_record):
        # old_instance_id = marc_record["004"].format_field()
        old_instance_id = marc_record["LKR"]["b"]
        if old_instance_id in self.instance_id_map:
            return self.instance_id_map[old_instance_id]["id"]
        else:
            add_stats(self.stats, "bib id not in map")
            raise ValueError(f"Old instance id not in map: {old_instance_id}")

    def handle_852s(self, f852s):
        ret = {
            "callNumberTypeId": "",
            "callNumberPrefix": "",
            "callNumber": "",
            "callNumberSuffix": "",
            "shelvingTitle": "",
            "copyNumber": "",
            "permanentLocationId": "",
        }

        for f852 in f852s:
            if "k" in f852 and f852["k"]:
                ret["callNumberPrefix"] = " ".join(f852.get_subfields("k")).strip()
            if "m" in f852 and f852["m"]:
                ret["callNumberSuffix"] = " ".join(f852.get_subfields("m"))
            if "l" in f852 and f852["l"]:
                ret["shelvingTitle"] = f852["l"]
            if "t" in f852 and f852["t"]:
                ret["copyNumber"] = f852["t"]
            if any(f852.get_subfields(*"hi")):
                ret["callNumber"] = " ".join(f852.get_subfields(*"hi"))
                sf2 = f852["2"] if "2" in f852 else ""
                ret["callNumberTypeId"] = self.get_call_number_type_id(
                    f852.indicator1, sf2
                )
            if "c" in f852 and f852["c"]:
                ret["permanentLocationId"] = self.get_location(f852)
        return ret

    def get_notes(self, marc_record):
        """returns the various notes fields from the marc record"""
        # TODO: UA First 852 will have a location the later could be set as notes
        for key, value in self.note_tags.items():
            note_type_name = value[1]
            note_type_id = self.get_holdings_note_type_id(note_type_name)
            staff_only = value[2]
            subfields = value[0]
            for field in marc_record.get_fields(key):
                note_value = value[3].join(field.get_subfields(*subfields))
                if note_value.strip():
                    yield {
                        "holdingsNoteTypeId": note_type_id,
                        "note": note_value,
                        "staffOnly": staff_only,
                    }

    def get_call_number_type_id(self, ind1, sf2):
        cnts = {
            "0": "Library of Congress classification",
            "1": "Dewey Decimal classification",
            "2": "National Library of Medicine classification",
            "3": "Superintendent of Documents classification",
            "4": "Shelving control number",
            "5": "Title",
            "6": "Shelved separately",
            "8": "Other scheme",
        }
        if ind1 == "7":
            if sf2 == "lcc":
                add_stats(
                    self.call_number_types_2, "Library of Congress classification"
                )
                return next(
                    t["id"]
                    for t in self.call_number_types
                    if t["name"] == "Library of Congress classification"
                )
        else:
            if str(ind1) in cnts:
                c_id = next(
                    (
                        t["id"]
                        for t in self.call_number_types
                        if t["name"] == cnts[str(ind1)]
                    ),
                    "",
                )
                if c_id:
                    add_stats(self.call_number_types_2, cnts[str(ind1)])
                    return c_id

        # add_stats(self.stats, f"Unhandled scheme in 852$2: {sf2} added other scheme")
        add_stats(self.call_number_types_2, f"{cnts[str(8)]} ({sf2})")
        return next(
            t["id"] for t in self.call_number_types if t["name"] == "Other scheme"
        )

    def get_location(self, f852):
        """returns the location mapped and translated"""
        if "c" in f852 and f852["c"]:
            try:
                legacy_code = f852["c"].strip()
                add_stats(self.legacy_locations, legacy_code)
                if self.location_map:
                    mapped_code = next(
                        (
                            l["folio_code"]
                            for l in self.location_map
                            if legacy_code == l["legacy_code"]
                        ),
                        "",
                    )
                    if not mapped_code:
                        raise ValueError(f"Legacy location not mapped: {legacy_code}")
                else:
                    mapped_code = legacy_code
                loc = next(
                    l["id"]
                    for l in self.folio_client.locations
                    if mapped_code == l["code"]
                )
                if not loc:
                    raise ValueError("Location code {mapped_code} not found in FOLIO")
                add_stats(self.folio_locations, mapped_code)
                return loc
            except Exception as ee:
                add_stats(self.stats, ee)
        return ""

    def get_holdingsStatements(self, marc_record, tag):
        """returns the various holdings statements"""
        for field in marc_record.get_fields(tag):
            yield {"statement": field["a"], "note": field["z"]}

    def get_holdings_note_type_id(self, note_type_name):
        """returns the note type id equivelant to the name"""
        nid = next(
            (
                t["id"]
                for t in self.holdings_note_types
                if t["name"].casefold() == note_type_name.casefold()
            ),
            "",
        )
        if not nid:
            print(f"Note type name not found {note_type_name}")
            return next(
                t["id"] for t in self.holdings_note_types if t["name"] == "Note"
            )
        else:
            return nid

    def validate(self, folio_holding, marc_record):
        failures = []
        for req in self.holdings_schema["required"]:
            if req not in folio_holding:
                failures.append(req)
                self.add_to_migration_report(
                    "Failed records that needs to get fixed",
                    f"Required field {req} is missing from {marc_record['001'].format_field()}",
                )
        if len(failures) > 0:
            add_stats(self.stats, "Records that failed validation")
            add_stats(self.stats, f"Records that failed validation {failures}")
            raise ValueError(
                f"Record {marc_record['001'].format_field()} failed validation {failures}"
            )


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
        print(f"{k} | {v}")


def fetch_holdings_schema():
    print("Fetching holdings schema...", end="")
    holdings_url = (
        "https://raw.githubusercontent.com/folio-org/mod-inventory-storage/"
        "master/ramls/holdingsrecord.json"
    )
    schema_request = requests.get(holdings_url)
    schema_text = schema_request.text
    print("done")
    return json.loads(schema_text)


def count_unmapped_fields(stats_map, schema, folio_object):
    schema_properties = schema["properties"].keys()
    unmatched_properties = (
        p for p in schema_properties if p not in folio_object.keys()
    )
    for p in unmatched_properties:
        add_stats(stats_map, p)


def count_mapped_fields(stats_map, folio_object):
    keys_to_delete = []
    for key, value in folio_object.items():
        if isinstance(value, str):
            if value:
                add_stats(stats_map, key)
            else:
                keys_to_delete.append(key)
        elif isinstance(value, list):
            if any(value):
                add_stats(stats_map, key)
            else:
                keys_to_delete.append(key)
        elif isinstance(value, dict):
            if any(value):
                add_stats(stats_map, key)
            else:
                keys_to_delete.append(key)

        else:
            print(type(value))

    for mykey in keys_to_delete:
        del folio_object[mykey]
