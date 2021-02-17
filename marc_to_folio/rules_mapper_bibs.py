"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import json
import logging
import time
from marc_to_folio.conditions import Conditions
import traceback
from logging import exception
import os.path
import uuid
import xml.etree.ElementTree as ET
from io import StringIO

import pymarc
from pymarc.record import Record
import requests
from pymarc import Field, JSONWriter

from marc_to_folio.rules_mapper_base import RulesMapperBase


class BibsRulesMapper(RulesMapperBase):
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    def __init__(
        self,
        folio_client,
        args,
    ):
        super().__init__(folio_client, Conditions(folio_client, self))
        self.folio = folio_client
        self.record_status = {}
        self.migration_report = {}
        self.suppress = args.suppress
        self.ils_flavour = args.ils_flavour
        self.holdings_map = {}
        self.id_map = {}
        self.srs_recs = []
        self.schema = self.instance_json_schema
        print("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())
        self.contrib_name_types = {}
        self.mapped_folio_fields = {}
        self.unmapped_folio_fields = {}
        self.alt_title_map = {}
        self.identifier_types = []
        print("Fetching mapping rules from the tenant")
        self.mappings = self.folio.folio_get_single_object("/mapping-rules")
        self.other_mode_of_issuance_id = next(
            (
                i["id"]
                for i in self.folio.modes_of_issuance
                if "unspecified" == i["name"].lower()
            )
        )
        self.unmapped_tags = {}
        self.unmapped_conditions = {}
        self.instance_relationships = {}
        self.instance_relationship_types = {}
        self.hrid_handling = self.folio.folio_get_single_object(
            "/hrid-settings-storage/hrid-settings"
        )
        self.hrid_prefix = self.hrid_handling["instances"]["prefix"]
        self.hrid_counter = self.hrid_handling["instances"]["startNumber"]
        print(f"Fetched HRID settings. HRID prefix is {self.hrid_prefix}")
        self.start = time.time()

    def parse_bib(self, marc_record: pymarc.Record, inventory_only=False):
        """Parses a bib recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://bit.ly/2S7Gyp3
         This is the main function"""
        self.print_progress()
        legacy_ids = self.get_legacy_id(marc_record, self.ils_flavour)
        folio_instance = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio.get_metadata_construct(),
        }
        id_map_string = ""
        self.add_to_migration_report(
            "Record status (leader pos 5)", marc_record.leader[5]
        )
        temp_inst_type = ""
        ignored_subsequent_fields = set()
        bad_tags = set()  # "907"

        for marc_field in marc_record:
            self.add_stats(self.stats, "Total number of Tags processed")
            self.report_bad_tags(marc_field, bad_tags)

            if marc_field.tag not in self.mappings and marc_field.tag not in ["008"]:
                self.report_legacy_mapping(marc_field.tag, True, False, True)
            else:
                if marc_field.tag not in ignored_subsequent_fields:
                    self.report_legacy_mapping(marc_field.tag, True, True, False)
                    if marc_field.tag == "880" and "6" in marc_field:
                        proxy_mapping = next(iter(self.mappings.get("880", [])))
                        if proxy_mapping and "fieldReplacementRule" in proxy_mapping:
                            target_field = next(
                                (
                                    r["targetField"]
                                    for r in proxy_mapping["fieldReplacementRule"]
                                    if r["sourceDigits"] == marc_field["6"][:3]
                                ),
                                "",
                            )
                            mappings = self.mappings.get(target_field, {})
                            self.add_to_migration_report(
                                "880 mappings",
                                f"Source digits: {marc_field['6'][:3]} Target field: {target_field}",
                            )
                        else:
                            mappings = []
                    else:
                        mappings = (
                            self.mappings.get(marc_field.tag, {})
                            if marc_field.tag != "880"
                            else []
                        )
                    if mappings:
                        self.map_field_according_to_mapping(
                            marc_field, mappings, folio_instance
                        )
                        if any(
                            m.get("ignoreSubsequentFields", False) for m in mappings
                        ):
                            ignored_subsequent_fields.add(marc_field.tag)
                    else:
                        self.add_to_migration_report(
                            "Mappings not found for field", marc_field.tag
                        )
                else:
                    self.report_legacy_mapping(marc_field.tag, True, False, True)

        self.perform_additional_parsing(
            folio_instance, marc_record, legacy_ids
        )
        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)

        self.validate(folio_instance, legacy_ids)
        self.dedupe_rec(folio_instance)
        # marc_record.remove_fields(*list(bad_tags))
        self.count_unmapped_fields(self.schema, folio_instance)
        try:
            self.count_mapped_fields(folio_instance)
        except Exception as ee:
            traceback.print_exc()
            print(ee)
            print(folio_instance)
        # TODO: trim away multiple whitespace and newlines..
        # TODO: createDate and update date and catalogeddate
        for legacy_id in legacy_ids:
            if legacy_id and self.ils_flavour in ["sierra", "iii", "907y"]:
                instance_level_call_number = (
                    marc_record["099"].format_field() if "099" in marc_record else ""
                )
                if instance_level_call_number:
                    self.add_to_migration_report(
                        "Instance level callNumber", bool(instance_level_call_number)
                    )
                id_map_string = json.dumps({"legacy_id": legacy_id, "folio_id": folio_instance["id"], "instanceLevelCallNumber": instance_level_call_number})
            elif legacy_id:
                id_map_string = json.dumps({"legacy_id": legacy_id, "folio_id": folio_instance["id"] })
            else:
                print(f"Legacy id is None {legacy_ids}")
        return folio_instance, id_map_string

    def perform_additional_parsing(
        self, folio_instance, marc_record, legacy_id
    ):
        """Do stuff not easily captured by the mapping rules"""
        folio_instance["source"] = "MARC"
        folio_instance["instanceFormatIds"] = list(
            set(self.get_instance_format_ids(marc_record, legacy_id))
        )

        folio_instance["instanceTypeId"] = self.get_instance_type_id(marc_record)
        
        folio_instance["modeOfIssuanceId"] = self.get_mode_of_issuance_id(
            marc_record, legacy_id
        )
        if "languages" in folio_instance:
            folio_instance["languages"].extend(self.get_languages(marc_record))
        else:
            folio_instance["languages"] = self.get_languages(marc_record)
        folio_instance["languages"] = list(
            self.filter_langs(folio_instance["languages"], marc_record)
        )
        self.handle_hrid(folio_instance, marc_record)
        folio_instance["discoverySuppress"] = bool(self.suppress)
        folio_instance["staffSuppress"] = bool(self.suppress)

        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)
        # self.validate(folio_instance)

    def wrap_up(self):
        print("Mapper wrapping up")

    def report_bad_tags(self, marc_field, bad_tags):
        if (
            (not marc_field.tag.isnumeric())
            and marc_field.tag != "LDR"
            and marc_field.tag not in bad_tags
        ):
            self.add_to_migration_report("Non-numeric tags in records", marc_field.tag)
            bad_tags.add(marc_field.tag)

    def get_instance_type_id(self, marc_record):
        return_id = ""
        def get_folio_id_by_name(f336a: str):
            match_template = f336a.lower().replace(" ", "")
            match = next(
                (
                    f["id"]
                    for f in self.folio.instance_types
                    if f["name"].lower().replace(" ", "") == match_template
                ),
                "",
            )
            if match:
                self.add_to_migration_report(
                    "Instance Type Mapping (336, 008)",
                    f"336$a -Successful matching on  {match_template} ({f336a})",
                )
            else:
                self.add_to_migration_report(
                    "Instance Type Mapping (336, 008)",
                    f"336$a -Unsuccessful matching on  {match_template} ({f336a})",
                )
            return match

        if not self.folio.instance_types:
            raise Exception("No instance_types setup in tenant")

        if "336" in marc_record and  "b" not in marc_record["336"]:
            self.add_to_migration_report(
                "Instance Type Mapping (336, 008)", f"Subfield b not in 336"
            )
            if "a" in marc_record["336"]:
                return_id = get_folio_id_by_name(marc_record["336"]["a"])
                

        if "336" in marc_record and  "b" in marc_record["336"]:
            t = self.conditions.get_ref_data_tuple_by_code(
                self.folio.instance_types, "instance_types", marc_record["336"]["b"]
            )
            if not t:                
                self.add_to_migration_report(
                    "Instance Type Mapping (336, 008)",
                    f'Code {marc_record["336"]["b"]} not found in FOLIO (from 336$b)',
                )
            else:
                self.add_to_migration_report(
                    "Instance Type Mapping (336, 008)", f"{t[1]} (from 336$b)"
                )
                return_id = t[0]
        
        if not return_id:
            t = self.conditions.get_ref_data_tuple_by_code(
                    self.folio.instance_types, "instance_types", "zzz"
                )
            return_id = t[0]
        return return_id

    def get_instance_format_ids(self, marc_record, legacy_id):
        # Lambdas
        def get_folio_id(code):
            return next(
                (f["id"] for f in self.folio.instance_formats if f["code"] == code),
                "",
            )

        def get_folio_id_by_name(f337a: str, f338a: str):
            f337a = f337a.lower().replace(" ", "")
            f338a = f338a.lower().replace(" ", "")
            match_template = f"{f337a} -- {f338a}"
            match = next(
                (
                    f["id"]
                    for f in self.folio.instance_formats
                    if f["name"] == match_template
                ),
                "",
            )
            if match:
                self.add_to_migration_report(
                    "Instance format ids handling (337 + 338)",
                    f"Successful matching on 337$a and 338$a - {match_template}",
                )
            else:
                self.add_to_migration_report(
                    "Instance format ids handling (337 + 338)",
                    f"Unsuccessful matching on 337$a and 338$a - {match_template}",
                )
            return match

        all_337s = marc_record.get_fields("337")
        all_338s = marc_record.get_fields("338")
        for fidx, f in enumerate(all_338s):
            source = f["2"] if "2" in f else "Not set"
            self.add_to_migration_report(
                "Instance format ids handling (337 + 338)",
                f"Source ($2) is set to {source}.",
            )
            if source.strip().startswith("rdacarrier"):
                logging.debug(f"Carrier is {source}")
                if "b" not in f and "a" in f:
                    self.add_to_migration_report(
                        "Instance format ids handling (337 + 338)", f"338$b is missing"
                    )
                    for sfidx, a in enumerate(f.get_subfields("a")):
                        corresponding_337 = (
                            all_337s[fidx] if fidx < len(all_337s) else None
                        )
                        if corresponding_337 and "a" in corresponding_337:
                            fmt_id = get_folio_id_by_name(corresponding_337["a"], a)
                            if fmt_id:
                                yield fmt_id

                for sfidx, b in enumerate(f.get_subfields("b")):
                    b = b.replace(" ", "")
                    if len(b) == 2:  # Normal 338b. should be able to map this
                        logging.debug(f"Length of 338 $b is 2")
                        yield get_folio_id(b)
                    elif len(b) == 1:
                        logging.debug(f"Length of 338 $b is 1 ")
                        corresponding_337 = (
                            all_337s[fidx] if fidx < len(all_337s) else None
                        )
                        if (
                            not corresponding_337
                        ):  # No matching 337. No use mapping the 338
                            logging.debug(f"No corresponding 337")
                            self.add_to_migration_report(
                                "Instance format ids handling (337 + 338))",
                                "No corresponding 337 to 338 even though 338$b was one charachter code",
                            )
                        else:  # Corresponding 337. Try to combine the codes.
                            logging.debug(f"Corresponding 337 found")
                            corresponding_b = (
                                corresponding_337.get_subfields("b")[sfidx]
                                if sfidx < len(corresponding_337.get_subfields("b"))
                                else None
                            ).strip()
                            if not corresponding_b:
                                logging.debug(f"No corresponding $b found")
                                self.add_to_migration_report(
                                    "Instance format ids handling (337 + 338))",
                                    "No corresponding $b in corresponding 338",
                                )
                            else:
                                logging.debug(f"Corresponding $b found")
                                combined_code = (corresponding_b + b).strip()
                                if len(combined_code) == 2:
                                    logging.debug(
                                        f"Combined codes are 2 chars long. Returning FOLIO ID"
                                    )
                                    yield get_folio_id(combined_code)

    def handle_hrid(self, folio_instance, marc_record):
        """Create HRID if not mapped. Add hrid as MARC record 001"""
        if "hrid" not in folio_instance:
            self.add_stats(self.stats, "Records without HRID from rules. Created HRID")
            num_part = str(self.hrid_counter).zfill(11)
            folio_instance["hrid"] = f"{self.hrid_prefix}{num_part}"
            self.hrid_counter += 1
        else:
            self.add_stats(self.stats, "Records with HRID from Rules")
        new_001 = Field(tag="001", data=folio_instance["hrid"])
        marc_record.remove_fields("001")
        marc_record.add_ordered_field(new_001)

    def get_mode_of_issuance_id(self, marc_record, legacy_id):
        level = marc_record.leader[7]
        try:
            name = "unspecified"
            if level in ["a", "c", "d", "m"]:
                name = "single unit"
            if level in ["b", "s"]:
                name = "serial"
            if level == "i":
                name = "integrating resource"
            if name == "unspecified":
                self.add_to_migration_report(
                    "unspecified Modes of issuance code", level
                )
            ret = next(
                (
                    i["id"]
                    for i in self.folio.modes_of_issuance
                    if str(name).lower() == i["name"].lower()
                ),
                "",
            )

            self.add_to_migration_report(
                "Matched Modes of issuance code", f"{ret} - {name}"
            )
            if not ret:
                self.add_to_migration_report(
                    "Unmatched Modes of issuance code",
                    level,
                )
                return self.other_mode_of_issuance_id
            return ret
        except IndexError:
            self.add_to_migration_report(
                "Possible cleaning tasks", f"No Leader[7] in {legacy_id}"
            )
            return self.other_mode_of_issuance_id
        except StopIteration as ee:
            print(
                f"StopIteration {marc_record.leader} {list(self.folio.modes_of_issuance)}"
            )
            raise ee

    def validate(self, folio_rec, legacy_ids):
        if not folio_rec.get("title", ""):
            raise ValueError(f"No title for {legacy_ids}")
        if not folio_rec.get("instanceTypeId", ""):
            raise ValueError(f"No Instance Type Id for {legacy_ids}")

    def get_nature_of_content(self, marc_record):
        return ["81a3a0e2-b8e5-4a7a-875d-343035b4e4d7"]

    def get_languages(self, marc_record):
        """Get languages and tranforms them to correct codes"""
        languages = set()
        lang_fields = marc_record.get_fields("041")
        if any(lang_fields):
            subfields = "abdefghjkmn"
            for lang_tag in lang_fields:
                if "2" in lang_tag:
                    self.add_to_migration_report(
                        "Language coude sources in 041", lang_tag["2"]
                    )
                lang_codes = lang_tag.get_subfields(*list(subfields))
                for lang_code in lang_codes:
                    lang_code = str(lang_code).lower().replace(" ", "")
                    langlength = len(lang_code)
                    if langlength == 3:
                        languages.add(lang_code.replace(" ", ""))
                    elif langlength > 3 and langlength % 3 == 0:
                        lc = lang_code.replace(" ", "")
                        new_codes = [lc[i : i + 3] for i in range(0, len(lc), 3)]
                        languages.update(new_codes)
                        languages.discard(lang_code)

                languages.update()
            languages = set(self.filter_langs(filter(None, languages), marc_record))
        elif "008" in marc_record and len(marc_record["008"].data) > 38:
            from_008 = "".join((marc_record["008"].data[35:38]))
            if from_008:
                languages.add(from_008.lower())
        # TODO: test agianist valide language codes
        return list(languages)

    def fetch_language_codes(self):
        """fetches the list of standardized language codes from LoC"""
        url = "https://www.loc.gov/standards/codelists/languages.xml"
        tree = ET.fromstring(requests.get(url).content)
        name_space = "{info:lc/xmlns/codelist-v1}"
        xpath_expr = "{0}languages/{0}language/{0}code".format(name_space)
        for code in tree.findall(xpath_expr):
            yield code.text

    def filter_langs(self, language_values, marc_record):
        forbidden_values = ["###", "zxx", "n/a", "N/A", "|||"]
        for language_value in language_values:
            if (
                language_value in self.language_codes
                and language_value not in forbidden_values
            ):
                yield language_value
            else:
                if language_value == "jap":
                    yield "jpn"
                elif language_value == "fra":
                    yield "fre"
                elif language_value == "sve":
                    yield "swe"
                elif language_value == "tys":
                    yield "ger"
                elif not language_value:
                    continue
                elif not language_value.strip():
                    continue
                elif language_value not in forbidden_values:
                    self.add_to_migration_report(
                        "Unrecognized language codes in records",
                        f"{language_value} not recognized for {self.get_legacy_id(marc_record, self.ils_flavour)}",
                    )

    def get_legacy_id(self, marc_record: Record, ils_flavour):
        if ils_flavour in ["iii", "sierra"]:
            return [marc_record["907"]["a"]]
        elif ils_flavour in ["907y"]:
            return [marc_record["907"]["y"]]
        elif ils_flavour == "035":
            return [marc_record["035"]["a"]]
        elif ils_flavour == "aleph":
            res = set()
            for f in marc_record.get_fields("998"):
                if "b" in f:
                    res.add(f["b"].strip())
            if any(res):
                return list(res)
            else:
                try:
                    ret = [marc_record["001"].format_field().strip()]
                    self.add_stats(self.stats, "Legacy id not found. 001 returned")
                    return ret
                except AttributeError:
                    self.add_stats(
                        self.stats, "Legacy id and 001 not found. Failing record "
                    )
                    raise ValueError("Legacy id and 001 not found. Failing record ")
        elif ils_flavour in ["voyager"]:
            return [marc_record["001"].format_field().strip()]
        else:
            raise Exception(f"ILS {ils_flavour} not configured")
