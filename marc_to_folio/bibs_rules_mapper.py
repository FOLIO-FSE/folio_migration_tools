"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import json
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


class BibsRulesMapper:
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    def __init__(
        self, folio, args,
    ):
        self.folio = folio
        self.suppress = args.suppress
        self.stats = {}
        self.record_status = {}
        self.migration_report = {}
        self.suppress = args.suppress
        self.conditions = BibsConditions(folio)
        self.instance_schema = get_instance_schema()
        self.ils_flavour = args.ils_flavour
        self.holdings_map = {}
        self.results_folder = args.results_folder
        self.id_map = {}
        self.srs_recs = []
        print("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())
        self.contrib_name_types = {}
        self.mapped_folio_fields = {}
        self.unmapped_folio_fields = {}
        self.alt_title_map = {}
        self.identifier_types = []
        self.mappings = self.folio.folio_get_single_object("/mapping-rules")
        self.srs_records_file = open(
            os.path.join(self.results_folder, "srs.json"), "w+"
        )
        self.srs_raw_records_file = open(
            os.path.join(self.results_folder, "srs_raw_records.json"), "w+"
        )
        self.srs_marc_records_file = open(
            os.path.join(self.results_folder, "srs_marc_records.json"), "w+"
        )
        self.marc_xml_writer = XMLWriter(
            open(os.path.join(self.results_folder, "marc_xml_dump.xml"), "wb+")
        )
        self.unmapped_tags = {}
        self.unmapped_conditions = {}
        self.instance_relationships = {}
        self.instance_relationship_types = {}

    def parse_bib(self, marc_record, inventory_only=False):
        """ Parses a bib recod into a FOLIO Inventory instance object
            Community mapping suggestion: https://bit.ly/2S7Gyp3
             This is the main function"""
        folio_instance = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio.get_metadata_construct(),
        }
        add_stats(self.record_status, marc_record.leader[5])
        temp_inst_type = ""
        ignored_subsequent_fields = set()
        bad_tags = {"039", "263", "229", "922"}  # "907"

        for marc_field in marc_record:
            add_stats(self.stats, "Total number of Tags processed")

            if (not marc_field.tag.isnumeric()) and marc_field.tag != "LDR":
                bad_tags.add(marc_field.tag)

            if marc_field.tag not in self.mappings and marc_field.tag not in ["008"]:
                add_stats(self.unmapped_tags, marc_field.tag)
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

        self.perform_additional_parsing(folio_instance, temp_inst_type, marc_record)
        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)
        self.validate(folio_instance, get_legacy_id(marc_record, self.ils_flavour))
        self.dedupe_rec(folio_instance)
        marc_record.remove_fields(*list(bad_tags))
        count_unmapped_fields(
            self.unmapped_folio_fields, self.instance_schema, folio_instance
        )
        try:
            count_mapped_fields(self.mapped_folio_fields, folio_instance)
        except:
            print(folio_instance)
        self.save_source_record(marc_record, folio_instance["id"])
        # TODO: trim away multiple whitespace and newlines..
        # TODO: createDate and update date and catalogeddate
        for legacy_id in get_legacy_id(marc_record, self.ils_flavour):
            self.id_map[legacy_id] = {"id": folio_instance["id"]}
        return folio_instance

    def perform_additional_parsing(self, folio_instance, temp_inst_type, marc_record):
        """Do stuff not easily captured by the mapping rules"""
        folio_instance["source"] = "MARC"
        if temp_inst_type and not folio_instance["instanceTypeId"]:
            folio_instance["instanceTypeId"] = temp_inst_type
        elif not temp_inst_type and not folio_instance.get("instanceTypeId", ""):
            raise ValueError("No Instance type ID")
        folio_instance["modeOfIssuanceId"] = self.get_mode_of_issuance_id(marc_record)
        if "languages" in folio_instance:
            folio_instance["languages"].extend(self.get_languages(marc_record))
        else:
            folio_instance["languages"] = self.get_languages(marc_record)
        folio_instance["languages"] = list(
            self.filter_langs(folio_instance["languages"], marc_record)
        )
        folio_instance["discoverySuppress"] = bool(self.suppress)
        folio_instance["staffSuppress"] = bool(self.suppress)

        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)
        # self.validate(folio_instance)

    def wrap_up(self):
        self.marc_xml_writer.close()
        self.flush_srs_recs()
        self.srs_records_file.close()
        self.srs_marc_records_file.close()
        self.srs_raw_records_file.close()

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
                add_stats(self.stats, f"Incomplete entity mapping - {marc_field.tag}")

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

    def save_source_record(self, marc_record, instance_id, suppress=False):
        """Saves the source Marc_record to the Source record Storage module"""
        srs_id = str(uuid.uuid4())
        marc_record.add_field(
            Field(
                tag="999",
                indicators=["f", "f"],
                subfields=["i", instance_id, "s", srs_id],
            )
        )
        self.srs_recs.append(
            (
                marc_record,
                instance_id,
                srs_id,
                self.folio.get_metadata_construct(),
                self.suppress,
            )
        )
        # if not suppress:
        # self.marc_xml_writer.write(marc_record)
        if len(self.srs_recs) == 1000:
            self.flush_srs_recs()
            self.srs_recs = []

    def flush_srs_recs(self):
        pool = ProcessPoolExecutor(max_workers=4)
        results = list(pool.map(get_srs_strings, self.srs_recs))
        self.srs_records_file.write("".join(r[0] for r in results))
        # self.srs_marc_records_file.write("".join(r[2] for r in results))
        # self.srs_raw_records_file.write("".join(r[1] for r in results))

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

    def add_to_migration_report(self, header, messageString):
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

    def dedupe_rec(self, rec):
        # remove duplicates
        for key, value in rec.items():
            if isinstance(value, list):
                res = []
                for v in value:
                    if v not in res:
                        res.append(v)
                rec[key] = res


def get_srs_strings(my_tuple):
    json_string = StringIO()
    writer = JSONWriter(json_string)
    writer.write(my_tuple[0])
    writer.close(close_fh=False)
    marc_uuid = str(uuid.uuid4())
    raw_uuid = str(uuid.uuid4())
    raw_record = {"id": my_tuple[2], "content": my_tuple[0].as_json()}
    parsed_record = {"id": my_tuple[2], "content": json.loads(my_tuple[0].as_json())}
    record = {
        "id": my_tuple[2],
        "deleted": False,
        "snapshotId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
        "matchedId": my_tuple[2],
        "generation": 0,
        "recordType": "MARC",
        "rawRecord": raw_record,
        "parsedRecord": parsed_record,
        "additionalInfo": {"suppressDiscovery": my_tuple[4]},
        "externalIdsHolder": {"instanceId": my_tuple[1]},
        "metadata": my_tuple[3],
        "state": "ACTUAL",
        "leaderRecordStatus": parsed_record["content"]["leader"][5],
    }
    if parsed_record["content"]["leader"][5] in [*"acdnposx"]:
        record["leaderRecordStatus"] = parsed_record["content"]["leader"][5]
    else:
        record["leaderRecordStatus"] = "d"
    return (
        f"{record['id']}\t{json.dumps(record)}\n",
        f"{raw_record['id']}\t{json.dumps(raw_record)}\n",
        f"{parsed_record['id']}\t{json.dumps(parsed_record)}\n",
    )


def grouped(iterable, n):
    "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
    return zip(*[iter(iterable)] * n)


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def get_legacy_id(marc_record, ils_flavour):
    if ils_flavour == "iii":
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


def count_unmapped_fields(stats_map, schema, folio_object):
    schema_properties = schema["properties"].keys()
    unmatched_properties = (
        p for p in schema_properties if p not in folio_object.keys()
    )
    for p in unmatched_properties:
        add_stats(stats_map, p)


def count_mapped_fields(stats_map, folio_object):
    for key, value in folio_object.items():
        if isinstance(value, bool) or value or any(value):
            add_stats(stats_map, key)
