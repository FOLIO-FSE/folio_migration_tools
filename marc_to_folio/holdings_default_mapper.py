"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
from marc_to_folio.migration_base import MigrationBase
import uuid
import requests
import logging
import json
import pymarc
from datetime import datetime


class HoldingsDefaultMapper(MigrationBase):
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    # Bootstrapping (loads data needed later in the script.)

    def __init__(self, folio_client, instance_id_map, args, location_map=None):
        logging.info("init Default Holdings mapper!")
        super().__init__()
        self.suppress = args.suppress
        self.tags_occurrences = {}
        self.location_map = location_map
        self.folio_client = folio_client
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}
        self.unmapped_holdings_fields = {}
        self.ils_flavour = args.ils_flavour
        self.call_number_types_ind2 = {}
        self.f852as = {}
        self.folio_locations = list(self.folio_client.locations)
        self.holdings_schema = fetch_holdings_schema()
        self.holdings_note_types = list(
            self.folio_client.folio_get_all("/holdings-note-types", "holdingsNoteTypes")
        )
        logging.info(f"{len(self.holdings_note_types)} note types")
        self.call_number_types = list(
            self.folio_client.folio_get_all("/call-number-types", "callNumberTypes")
        )
        # Send out a list of note types that should be either public or private
        self.note_tags = {
            "506": ("3abcdefgqu", "Note", False, " "),  # Was: Restriction
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
            "876": ("p3", "Note", False, " | "),  # Was: bound with item data
        }

    def parse_hold(self, marc_record):
        """Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: """
        self.add_stats(self.stats, "Number of records in file(s)")
        for f in marc_record:
            self.add_stats(self.tags_occurrences, f.tag)

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
            "discoverySuppress": self.suppress,
        }
        rec.update(self.handle_852s(f852s))
        self.holdings_id_map[marc_record["001"].format_field()] = rec["id"]
        self.count_unmapped_fields(
            self.holdings_schema, rec,
        )
        self.count_mapped_fields(rec)
        self.validate(rec, marc_record)
        return rec

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.holdings_id_map:
            del self.holdings_id_map[id_key]

    def report_on_852s(self, marc_record):
        if "852" not in marc_record:
            self.add_stats(self.stats, "Records missing 852")
            raise ValueError(f'852 missing for {marc_record["001"]}')
        f852s = marc_record.get_fields("852")
        if len(f852s) > 1:
            self.add_stats(self.stats, "Records with multiple 852 fields")
            for f852 in f852s:
                for sf in f852.get_subfields():
                    self.add_stats(self.stats, f"852 - {sf}")
        return f852s

    def wrap_up(self):
        print("# Holdings transformation results")
        print("## Stats")
        self.print_dict_to_md_table(self.stats)
        self.write_migration_report()
        self.print_mapping_report()

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
        old_instance_id = ""
        marc_field = ""
        if self.ils_flavour == "aleph":
            old_instance_id = marc_record["LKR"]["b"]
            marc_field = marc_record["LKR"]
        elif self.ils_flavour == "voyager":
            old_instance_id = marc_record["004"].format_field()
            marc_field = marc_record["004"]
        else:
            raise Exception("Ils flavour not found!")
        #
        if old_instance_id in self.instance_id_map:
            return self.instance_id_map[old_instance_id]["id"]
        else:
            self.add_stats(self.stats, "bib id not in map")
            raise ValueError(
                f"Old instance id not in map: {old_instance_id} Field: {marc_field}"
            )

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
                self.add_to_migration_report("Call number types values (852 $2)", sf2)
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
                    self.add_to_migration_report(
                        "Call number types values ind1", cnts.get(ind1, "")
                    )
                    return c_id

        self.add_to_migration_report(
            "Call number types values (852 $2)", f"{cnts.get('8')} ({sf2})"
        )
        return next(
            t["id"] for t in self.call_number_types if t["name"] == "Other scheme"
        )

    def get_location(self, f852):
        """returns the location mapped and translated"""
        loc_subfield = {"aleph": "c", "voyager": "b"}.get(self.ils_flavour)
        if loc_subfield not in f852:
            self.add_stats(self.stats, f"Records without ${loc_subfield} in 852")
            return ""
        legacy_code = f852[loc_subfield]
        try:
            self.add_to_migration_report("Legacy location codes", legacy_code)
            if self.location_map and any(self.location_map):
                mapped_code = next(
                    (
                        l["folio_code"]
                        for l in self.location_map
                        if legacy_code == l["legacy_code"]
                    ),
                    "",
                )
                if not mapped_code:
                    self.add_to_migration_report(
                        "Locations - Unmapped legacy locations", legacy_code
                    )
                    mapped_code = "tech"
                    # raise ValueError(f"Legacy location not mapped: {legacy_code}")
            else:
                mapped_code = legacy_code
            loc = next(
                l["id"] for l in self.folio_locations if mapped_code == l["code"]
            )
            if not loc:
                self.add_to_migration_report(
                    "Locations - Mapped location not in FOLIO", mapped_code
                )
                raise ValueError("Location code {mapped_code} not found in FOLIO")
            self.add_to_migration_report(
                "Locations - Successfully Mapped locations", mapped_code
            )
            return loc
        except Exception as ee:
            logging.error(f"location not found: {ee}")
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
            logging.warning(f"Note type name not found {note_type_name}")
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
            self.add_stats(self.stats, "Records that failed validation")
            self.add_stats(self.stats, f"Records that failed validation {failures}")
            raise ValueError(
                f"Record {marc_record['001'].format_field()} failed validation {failures}"
            )


def fetch_holdings_schema():
    logging.info("Fetching holdings schema...", end="")
    holdings_url = (
        "https://raw.githubusercontent.com/folio-org/mod-inventory-storage/"
        "master/ramls/holdingsrecord.json"
    )
    schema_request = requests.get(holdings_url)
    schema_text = schema_request.text
    logging.info("done")
    return json.loads(schema_text)

