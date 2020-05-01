"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import uuid
import logging
import json
from marc_to_folio.holdings_default_mapper import HoldingsDefaultMapper
from datetime import datetime


class HoldingsAlabamaMapper(HoldingsDefaultMapper):
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    # Bootstrapping (loads data needed later in the script.)

    def __init__(self, folio, instance_id_map, location_map=None):
        print("init ALABAMA mapper!")
        self.folio = folio
        self.migration_user_id = "d916e883-f8f1-4188-bc1d-f0dce1511b50"
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}
        self.call_number_types_2 = {}
        self.call_number_types_ind1 = {}
        self.call_number_types_ind2 = {}
        self.f852as = {}
        self.folio_locations = {}
        self.legacy_locations = {}
        self.unmapped_locations = {}
        self.stats = {"bib id not in map": 0, "multiple 852s": 0}
        # Send out a list of note types that should be either public or private
        self.note_tags = {
            "506": "abcdefu23568",
            "538": "aiu3568",
            "541": "abcdefhno3568",
            "561": "au3568",
            "562": "abcde3568",
            "563": "au3568",  # Binding note type
            "583": "abcdefhijklnouxz23568",  # Map to Action note
            "843": "abcdefmn35678",
            "845": "abcdu3568",
            "852": "z",
        }
        self.nonpublic_note_tags = {"852": "x"}

    def parse_hold(self, marc_record):
        """Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: """
        f852s = self.handle_852s(marc_record)
        rec = {
            "id": str(uuid.uuid4()),
            "metadata": super().get_metadata_construct(self.migration_user_id),
            "hrid": marc_record["001"].format_field(),
            "instanceId": self.get_instance_id(marc_record),
            "permanentLocationId": self.get_location(f852s[0]),
            "holdingsStatements": list(self.get_holdingsStatements(marc_record)),
            # 'holdingsTypeId': '',
            # 'formerIds': list(set()),
            # 'temporaryLocationId': '',
            # 'electronicAccess':,
            # 'acquisitionFormat': '', - NOT USED IN UA
            # TODO: 'acquisitionMethod': '',
            # TODO: 'receiptStatus': '',
            "notes": list(self.get_notes(marc_record)),
            # 'illPolicyId': '',
            # 'retentionPolicy': '',
            # 'digitizationPolicy': '',            #
            # 'numberOfItems': '',
            # 'discoverySuppress': '',
            # 'statisticalCodeIds': [],
            # 'holdingsItems': [],
            # holdingsInstance
            # receivingHistory
            # holdingsStatementsForSupplements
            # holdingsStatementsForIndexes
        }
        rec.update(self.get_callnumber_data(f852s[0]))
        self.holdings_id_map[marc_record["001"].format_field()] = rec["id"]
        return rec

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.id_map:
            del self.holdings_id_map[id_key]

    def handle_852s(self, marc_record):
        if "852" not in marc_record:
            raise ValueError(f'852 missing for {marc_record["001"]}')
        f852s = marc_record.get_fields("852")
        if len(f852s) > 1:
            self.stats["multiple 852s"] += 1
        if "a" in f852s[0]:
            print(f"852$a in {marc_record['001']} - {f852s[0]['a']}")
        if "b" not in f852s[0]:
            raise ValueError(f'No 852$b in {marc_record["001"]}')
        # Holler if j or k or m is apparent
        if "k" in f852s[0]:
            print(f"852$k in {marc_record['001']} - {f852s[0]['k']}")
        if "j" in f852s[0]:
            print(f"852$j in {marc_record['001']} - {f852s[0]['j']}")
        if "m" in f852s[0]:
            print(f"852$m in {marc_record['001']} - {f852s[0]['m']}")
        if "2" in f852s[0]:
            if f852s[0]["2"] in self.call_number_types_2:
                self.call_number_types_2[f852s[0]["2"]] += 1
            else:
                self.call_number_types_2[f852s[0]["2"]] = 1
        if "a" in f852s[0]:
            if f852s[0]["a"] in self.f852as:
                self.f852as[f852s[0]["a"]] += 1
            else:
                self.f852as[f852s[0]["a"]] = 1
        if f852s[0].indicator1:
            if f852s[0].indicator1 in self.call_number_types_ind1:
                self.call_number_types_ind1[f852s[0].indicator1] += 1
            else:
                self.call_number_types_ind1[f852s[0].indicator1] = 1
        if f852s[0].indicator2:
            if f852s[0].indicator2 in self.call_number_types_ind2:
                self.call_number_types_ind2[f852s[0].indicator2] += 1
            else:
                self.call_number_types_ind2[f852s[0].indicator2] = 1
        return f852s

    def wrap_up(self):
        print("STATS:")
        print(json.dumps(self.stats, indent=4))
        print("LEGACY LOCATIONS:")
        print(json.dumps(self.legacy_locations, indent=4))
        print("FOLIO LOCATIONS:")
        print(json.dumps(self.folio_locations, indent=4))
        print("UNMAPPED LOCATION CODES:")
        print(json.dumps(self.unmapped_locations, indent=4))
        print("CALL NUMBER TYPES (852 $2) values:")
        print(json.dumps(self.call_number_types_2, indent=4))
        print("CALL NUMBER TYPES (852 ind1) values:")
        print(json.dumps(self.call_number_types_ind1, indent=4))
        print("CALL NUMBER TYPES (852 ind2) values:")
        print(json.dumps(self.call_number_types_ind2, indent=4))

    def get_instance_id(self, marc_record):
        old_instance_id = marc_record["004"].format_field()
        if old_instance_id in self.instance_id_map:
            return self.instance_id_map[old_instance_id]["id"]
        else:
            self.stats["bib id not in map"] += 1
            logging.warn(f"Old instance id not in map: {old_instance_id}")
            raise Exception(f"Old instance id not in map: {old_instance_id}")

    def get_callnumber_data(self, f852):
        # TODO: handle repeated 852s
        # read and implement http://www.loc.gov/marc/holdings/hd852.html
        # TODO: UA does not use $2
        # TODO: UA First 852 will have a location the later could be set as notes
        # TODO: print call number type id sources (852 ind1 and $2) no $2 in UA
        # if ind2 present, holler/count
        # report on use of $l

        return {
            "callNumberTypeId": "fccfdd98-e061-49ed-8185-c13979fbfaa2",
            "callNumberPrefix": " ".join(f852.get_subfields("k")),
            "callNumber": " ".join(f852.get_subfields(*"hij")),
            "callNumberSuffix": " ".join(f852.get_subfields(*"m")),
            "shelvingTitle": " ".join(f852.get_subfields(*"l")),
            "copyNumber": " ".join(f852.get_subfields(*"t")),
        }

    def get_notes(self, marc_record):
        """returns the various notes fields from the marc record"""
        # TODO: UA First 852 will have a location the later could be set as notes
        for key, value in self.note_tags.items():
            for field in marc_record.get_fields(key):
                nv = " ".join(field.get_subfields(*value))
                if nv.strip():
                    yield {
                        # TODO: add logic for noteTypeId
                        "holdingsNoteTypeId": "b160f13a-ddba-4053-b9c4-60ec5ea45d56",
                        "note": " ".join(field.get_subfields(*value)),
                        "staffOnly": False,
                    }
        for key, value in self.nonpublic_note_tags.items():
            for field in marc_record.get_fields(key):
                nv = " ".join(field.get_subfields(*value))
                if nv.strip():
                    yield {
                        # TODO: add logic for noteTypeId
                        "holdingsNoteTypeId": "b160f13a-ddba-4053-b9c4-60ec5ea45d56",
                        "note": " ".join(field.get_subfields(*value)),
                        "staffOnly": True,
                    }

    def get_location(self, f852):
        """returns the location mapped and translated"""
        return self.folio.get_location_id(f852["b"])

    def get_holdingsStatements(self, marc_record):
        """returns the various holdings statements"""
        yield {"statement": "Some statement", "note": "some note"}
