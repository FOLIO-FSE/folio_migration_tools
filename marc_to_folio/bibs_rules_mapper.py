"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import json
import logging
from marc_to_folio.migration_base import MigrationBase
import os.path
import pymarc
import copy
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor
from io import StringIO
from textwrap import wrap

import requests
from pymarc import Field, JSONWriter, XMLWriter

from marc_to_folio.bibs_conditions import BibsConditions


class BibsRulesMapper(MigrationBase):
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    def __init__(
        self, folio, args,
    ):
        super().__init__()
        self.folio = folio
        self.processed = 0
        self.record_status = {}
        self.migration_report = {}
        self.suppress = args.suppress
        self.conditions = BibsConditions(folio, self)
        self.instance_schema = get_instance_schema()
        self.ils_flavour = args.ils_flavour
        self.holdings_map = {}
        self.id_map = {}
        self.srs_recs = []
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

    def parse_bib(self, marc_record, inventory_only=False):
        """ Parses a bib recod into a FOLIO Inventory instance object
            Community mapping suggestion: https://bit.ly/2S7Gyp3
             This is the main function"""
        self.processed += 1
        self.print_progress(self.processed)
        legacy_id = get_legacy_id(marc_record, self.ils_flavour)
        folio_instance = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio.get_metadata_construct(),
        }
        self.add_to_migration_report(
            "Record status (leader pos 5)", marc_record.leader[5]
        )
        temp_inst_type = ""
        ignored_subsequent_fields = set()
        bad_tags = {}  # "907"

        for marc_field in marc_record:
            self.add_stats(self.stats, "Total number of Tags processed")

            if (not marc_field.tag.isnumeric()) and marc_field.tag != "LDR":
                bad_tags.add(marc_field.tag)

            if marc_field.tag not in self.mappings and marc_field.tag not in ["008"]:
                self.report_legacy_mapping(marc_field.tag, False, False)
            else:
                if marc_field.tag not in ignored_subsequent_fields:
                    mappings = self.mappings[marc_field.tag]
                    self.map_field_according_to_mapping(
                        marc_field, mappings, folio_instance
                    )
                    if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                        ignored_subsequent_fields.add(marc_field.tag)

            if marc_field.tag == "008":
                temp_inst_type = folio_instance["instanceTypeId"]

        self.perform_additional_parsing(
            folio_instance, temp_inst_type, marc_record, legacy_id
        )
        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)

        self.validate(folio_instance, legacy_id)
        self.dedupe_rec(folio_instance)
        marc_record.remove_fields(*list(bad_tags))
        self.count_unmapped_fields(self.instance_schema, folio_instance)
        try:
            self.count_mapped_fields(folio_instance)
        except:
            print(folio_instance)
        # TODO: trim away multiple whitespace and newlines..
        # TODO: createDate and update date and catalogeddate
        for legacy_id in legacy_id:
            self.id_map[legacy_id] = {"id": folio_instance["id"]}
        return folio_instance

    def perform_additional_parsing(
        self, folio_instance, temp_inst_type, marc_record, legacy_id
    ):
        """Do stuff not easily captured by the mapping rules"""
        folio_instance["source"] = "MARC"
        folio_instance["instanceFormatIds"] = list(
            set(self.get_instance_format_ids(marc_record, legacy_id))
        )
        if temp_inst_type and not folio_instance["instanceTypeId"]:
            folio_instance["instanceTypeId"] = temp_inst_type
        elif not temp_inst_type and not folio_instance.get("instanceTypeId", ""):
            raise ValueError("No Instance type ID")
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

    def get_instance_format_ids(self, marc_record, legacy_id):
        # Lambdas
        get_folio_id = lambda code: next(
            (f["id"] for f in self.folio.instance_formats if f["code"] == code), "",
        )
        all_337s = marc_record.get_fields("337")
        all_338s = marc_record.get_fields("338")
        for fidx, f in enumerate(all_338s):
            source = f["2"] if "2" in f else "Not set"
            self.add_to_migration_report(
                "Instance format ids handling (337 + 338)",
                f"Source ($2) is set to {source}",
            )
            if source == "rdacarrier":
                logging.debug(f"Carrier is {source}")
                for sfidx, b in enumerate(f.get_subfields("b")):
                    if len(b) == 2:  # Normal 338b. should be able to map this
                        logging.debug(f"Length of 338 $b is 2")
                        yield get_folio_id(b)
                    elif len(b) == 1:
                        logging.debug(f"Length of 338 $b is 1 ")
                        corresponding_337 = (
                            all_337s[fidx] if fidx < len(all_337s) else None
                        )
                        if not corresponding_337:
                            logging.debug(f"No corresponding 337")
                            self.add_to_migration_report(
                                "Instance format ids handling (337 + 338))",
                                "No corresponding 337 to 338 even though 338$b was one charachter code",
                            )
                        else:
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

    def map_field_according_to_mapping(self, marc_field, mappings, rec):
        for mapping in mappings:
            if "entity" not in mapping:
                target = mapping["target"]
                if has_conditions(mapping):
                    values = self.apply_rules(marc_field, mapping)
                    # TODO: add condition to customize this...
                    if marc_field.tag == "655":
                        values[0] = f"Genre: {values[0]}"
                    self.add_value_to_target(rec, target, values)
                elif has_value_to_add(mapping):
                    value = [mapping["rules"][0]["value"]]
                    self.add_value_to_target(rec, target, value)
                else:
                    value = marc_field.format_field() if marc_field else ""
                    self.add_value_to_target(rec, target, [value])
            else:
                e_per_subfield = mapping.get("entityPerRepeatedSubfield", False)
                self.handle_entity_mapping(
                    marc_field, mapping["entity"], rec, e_per_subfield
                )

    def handle_entity_mapping(self, marc_field, entity_mapping, rec, e_per_subfield):
        e_parent = entity_mapping[0]["target"].split(".")[0]
        if e_per_subfield:
            for sf_tuple in grouped(marc_field.subfields, 2):
                temp_field = Field(
                    tag=marc_field.tag,
                    indicators=marc_field.indicators,
                    subfields=[sf_tuple[0], sf_tuple[1]],
                )
                entity = self.create_entity(entity_mapping, temp_field, e_parent)
                if type(entity) is dict and any(entity.values()):
                    self.add_entity_to_record(entity, e_parent, rec)
                elif type(entity) is list and any(entity):
                    self.add_entity_to_record(entity, e_parent, rec)
        else:
            entity = self.create_entity(entity_mapping, marc_field, e_parent)
            if all(entity.values()) or e_parent == "electronicAccess":
                self.add_entity_to_record(entity, e_parent, rec)
            else:
                self.add_to_migration_report(
                    "Incomplete entity mapping (a code issue)", marc_field.tag
                )

    def handle_hrid(self, folio_instance, marc_record):
        """Create HRID if not mapped. Add hrid as MARC record 001"""
        if "hrid" not in folio_instance:
            self.add_stats(self.stats, "Records without HRID from rules. Created HRID")
            folio_instance["hrid"] = f"{self.hrid_prefix}{self.hrid_counter}"
            self.hrid_counter += 1
        else:
            self.add_stats(self.stats, "Records with HRID from Rules")
        new_001 = Field(tag="001", data=folio_instance["hrid"])

        marc_record.remove_fields("001")
        marc_record.add_ordered_field(new_001)

    def create_entity(self, entity_mappings, marc_field, entity_parent_key):
        entity = {}
        for entity_mapping in entity_mappings:
            k = entity_mapping["target"].split(".")[-1]
            values = self.apply_rules(marc_field, entity_mapping)
            if values:
                if entity_parent_key == k:
                    entity = values[0]
                else:
                    entity[k] = values[0]
        return entity

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
                    "Unmatched Modes of issuance code", level,
                )
                return self.other_mode_of_issuance_id
            return ret
        except IndexError:
            self.add_to_migration_report(
                "Possible cleaning tasks" f"No Leader[7] in {legacy_id}"
            )
            return self.other_mode_of_issuance_id
        except StopIteration as ee:
            print(
                f"StopIteration {marc_record.leader} {list(self.folio.modes_of_issuance)}"
            )
            raise ee

    def add_entity_to_record(self, entity, entity_parent_key, rec):
        sch = self.instance_schema["properties"]
        if sch[entity_parent_key]["type"] == "array":
            if entity_parent_key not in rec:
                rec[entity_parent_key] = [entity]
            else:
                rec[entity_parent_key].append(entity)
        else:
            rec[entity_parent_key] = entity

    def apply_rules(self, marc_field, mapping):
        values = []
        value = ""
        if has_conditions(mapping):
            c_type_def = mapping["rules"][0]["conditions"][0]["type"].split(",")
            condition_types = [x.strip() for x in c_type_def]
            parameter = mapping["rules"][0]["conditions"][0].get("parameter", {})
            # print(f'conditions {condition_types}')
            if mapping.get("applyRulesOnConcatenatedData", ""):
                value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
                value = self.apply_rule(value, condition_types, marc_field, parameter)
            else:
                if mapping.get("subfield", []):
                    if mapping.get("ignoreSubsequentFields", False):
                        sfs = []
                        for sf in mapping["subfield"]:
                            next_subfield = next(iter(marc_field.get_subfields(sf)), "")
                            sfs.append(next_subfield)
                        value = " ".join(
                            [
                                self.apply_rule(
                                    x, condition_types, marc_field, parameter
                                )
                                for x in sfs
                            ]
                        )
                    else:
                        subfields = marc_field.get_subfields(*mapping["subfield"])
                        x = [
                            self.apply_rule(x, condition_types, marc_field, parameter)
                            for x in subfields
                        ]
                        value = " ".join(set(x))
                else:
                    value1 = marc_field.format_field() if marc_field else ""
                    value = self.apply_rule(
                        value1, condition_types, marc_field, parameter
                    )
        elif has_value_to_add(mapping):
            return [mapping["rules"][0]["value"]]
        elif not mapping.get("rules", []) or not mapping["rules"][0].get(
            "conditions", []
        ):
            value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
        if mapping.get("subFieldSplit", ""):
            values = wrap(value, 3)
        else:
            values = [value]
        return values

    def apply_rule(self, value, condition_types, marc_field, parameter):
        v = value
        for condition_type in condition_types:
            v = self.conditions.get_condition(condition_type, v, parameter, marc_field)
        return v

    def add_value_to_target(self, rec, target_string, value):
        if value:
            targets = target_string.split(".")
            if len(targets) == 1:
                self.add_value_to_first_level_target(rec, target_string, value)
            else:
                sc_parent = None
                parent = None
                sch = self.instance_schema["properties"]
                sc_prop = sch
                prop = copy.deepcopy(rec)
                for target in targets:  # Iterate over names in hierarcy
                    if target in sc_prop:  # property is on this level
                        sc_prop = sc_prop[target]  # set current property
                    else:  # next level. take the properties from the items
                        sc_prop = sc_parent["items"]["properties"][target]
                    if (
                        target not in rec and not sc_parent
                    ):  # have we added this already?
                        if is_array_of_strings(sc_prop):
                            rec[target] = []
                            # break
                            # prop[target].append({})
                        elif is_array_of_objects(sc_prop):
                            rec[target] = [{}]
                            # break
                        elif (
                            sc_parent
                            and is_array_of_objects(sc_parent)
                            and sc_prop.get("type", "string") == "string"
                        ):
                            # print(f"break! {target} {prop} {value}")
                            if len(rec[parent][-1]) > 0:
                                rec[parent][-1][target] = value[0]
                            else:
                                rec[parent][-1] = {target: value[0]}
                            # print(parent)
                            # break
                        else:
                            if sc_parent["type"] == "array":
                                prop[target] = {}
                                parent.append(prop[target])
                            else:
                                raise Exception(
                                    f"Edge! {target_string} {sch[target_string]}"
                                )
                    else:  # We already have stuff in here
                        if is_array_of_objects(sc_prop) and len(rec[target][-1]) == len(
                            sc_prop["items"]["properties"]
                        ):
                            rec[target].append({})
                        elif sc_parent and target in rec[parent][-1]:
                            rec[parent].append({})
                            if len(rec[parent][-1]) > 0:
                                rec[parent][-1][target] = value[0]
                            else:
                                rec[parent][-1] = {target: value[0]}
                        elif (
                            sc_parent
                            and is_array_of_objects(sc_parent)
                            and sc_prop.get("type", "string") == "string"
                        ):
                            # print(f"break! {target} {prop} {value}")
                            if len(rec[parent][-1]) > 0:
                                rec[parent][-1][target] = value[0]
                                # print(rec[parent])
                            else:
                                rec[parent][-1] = {target: value[0]}
                                # print(rec[parent])
                        # break

                    # if target == targets[-1]:
                    # print(f"HIT {target} {value[0]}")
                    # prop[target] = value[0]
                    # prop = rec[target]
                    sc_parent = sc_prop
                    parent = target

    def add_value_to_first_level_target(self, rec, target_string, value):
        # print(f"{target_string} {value} {rec}")
        sch = self.instance_schema["properties"]
        if (
            sch[target_string]["type"] == "array"
            and sch[target_string]["items"]["type"] == "string"
        ):
            if target_string not in rec:
                rec[target_string] = value
            else:
                rec[target_string].extend(value)
        elif sch[target_string]["type"] == "string":
            rec[target_string] = value[0]
        else:
            raise Exception(f"Edge! {target_string} {sch[target_string]['type']}")

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
                        f"{language_value} not recognized for {get_legacy_id(marc_record, self.ils_flavour)}",
                    )

    def dedupe_rec(self, rec):
        # remove duplicates
        for key, value in rec.items():
            if isinstance(value, list):
                res = []
                for v in value:
                    if v not in res:
                        res.append(v)
                rec[key] = res


def grouped(iterable, n):
    "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
    return zip(*[iter(iterable)] * n)


def get_legacy_id(marc_record, ils_flavour):
    if ils_flavour in ["iii", "sierra"]:
        return [marc_record["907"]["a"]]
    elif ils_flavour == "035":
        return [marc_record["035"]["a"]]
    elif ils_flavour == "aleph":
        res = set()
        for f in marc_record.get_fields("998"):
            res.add(f["b"].strip())
        return list(res)
    elif ils_flavour in ["voyager"]:
        return [marc_record["001"].format_field().strip()]
    else:
        raise Exception(f"ILS {ils_flavour} not configured")


def has_conditions(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("conditions", [])


def has_value_to_add(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("value", "")


def get_instance_schema():
    # instance_url = "https://raw.githubusercontent.com/folio-org/mod-inventory-storage/master/ramls/instance.json"
    instance_url = "https://raw.githubusercontent.com/folio-org/mod-inventory/master/ramls/instance.json"
    schema_request = requests.get(instance_url)
    schema_text = schema_request.text
    return json.loads(schema_text)


def is_array_of_strings(schema_property):
    sc_prop_type = schema_property.get("type", "string")
    return sc_prop_type == "array" and schema_property["items"]["type"] == "string"


def is_array_of_objects(schema_property):
    sc_prop_type = schema_property.get("type", "string")
    return sc_prop_type == "array" and schema_property["items"]["type"] == "object"

