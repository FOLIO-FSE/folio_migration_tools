"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import json
import os.path
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor
from io import StringIO
from textwrap import wrap

import requests
from pymarc import Field, JSONWriter, XMLWriter

from marc_to_folio.conditions import Conditions


class RulesMapper:
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    def __init__(self, folio, results_path):
        self.folio = folio
        self.conditions = Conditions(folio)
        self.migration_user_id = "d916e883-f8f1-4188-bc1d-f0dce1511b50"
        instance_url = "https://raw.githubusercontent.com/folio-org/mod-inventory-storage/master/ramls/instance.json"
        schema_request = requests.get(instance_url)
        schema_text = schema_request.text
        self.instance_schema = json.loads(schema_text)
        self.holdings_map = {}
        self.results_path = results_path
        # TODO: Move SRS Handling to somewhere more central
        self.id_map = {}
        self.srs_recs = []
        print("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())
        self.contrib_name_types = {}
        self.mapped_folio_fields = {}
        self.alt_title_map = {}
        self.identifier_types = []
        self.misc_stats = {}
        self.mappings = self.folio.folio_get_single_object("/mapping-rules")
        self.srs_records_file = open(os.path.join(self.results_path, "srs.json"), "w+")
        self.srs_raw_records_file = open(
            os.path.join(self.results_path, "srs_raw_records.json"), "w+"
        )
        self.srs_marc_records_file = open(
            os.path.join(self.results_path, "srs_marc_records.json"), "w+"
        )
        self.marc_xml_writer = XMLWriter(
            open(os.path.join(self.results_path, "marc_xml_dump.xml"), "wb+")
        )
        self.unmapped_tags = {}
        self.unmapped_conditions = {}
        self.instance_relationships = {}
        self.instance_relationship_types = {}

    def parse_bib(self, marc_record, record_source, inventory_only=False):
        """ Parses a bib recod into a FOLIO Inventory instance object
            Community mapping suggestion: https://bit.ly/2S7Gyp3
             This is the main function"""
        folio_instance = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio.get_metadata_construct(),
        }
        temp_inst_type = ""
        ignored_subsequent_fields = set()
        bad_tags = {"039", "263", "229", "922", "945"}  # "907"
        raise Exception("Ta fram alla ok 945")
        raise Exception("Räkna utan 945")
        raise Exception("Undanhåll sådant som ska suppressas")
        raise Exception("lägg in FOLIO-locations:arna")
        for marc_field in marc_record:
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
        # self.validate(folio_instance)
        self.dedupe_rec(folio_instance)
        marc_record.remove_fields(*list(bad_tags))
        if not inventory_only:
            self.save_source_record(marc_record, folio_instance["id"])
        else:
            add_stats(self.misc_stats, "inventory_only")

        """
        raise Exception("trim away multiple whitespace and newlines..")
        raise Exception('createDate and update date and catalogeddate')"""
        self.id_map[get_source_id(marc_record)] = {"id": folio_instance["id"]}
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

        # folio_instance['natureOfContentTermIds'] = self.get_nature_of_content(
        #     marc_record)
        # self.validate(folio_instance)

    def wrap_up(self):
        self.marc_xml_writer.close()
        self.flush_srs_recs()
        self.srs_records_file.close()
        self.srs_marc_records_file.close()
        self.srs_raw_records_file.close()
        sorted_tags = {k: self.unmapped_tags[k] for k in sorted(self.unmapped_tags)}
        print(json.dumps(sorted_tags, indent=4))
        print(json.dumps(self.unmapped_conditions, indent=4))

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
            if any(entity.values()):
                self.add_entity_to_record(entity, e_parent, rec)

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
                            sfs.append(next(iter(marc_field.get_subfields(sf)), ""))
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
                        value = " ".join(
                            [
                                self.apply_rule(
                                    x, condition_types, marc_field, parameter
                                )
                                for x in subfields
                            ]
                        )
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
            sch = self.instance_schema["properties"]
            prop = rec
            sc_prop = sch
            sc_parent = None
            parent = None
            if len(targets) == 1:
                # print(f"{target_string} {value} {rec}")
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
                    raise Exception(
                        f"Edge! {target_string} {sch[target_string]['type']}"
                    )
            else:
                for target in targets:
                    if target in sc_prop:
                        sc_prop = sc_prop[target]
                    else:
                        sc_prop = sc_parent["items"]["properties"][target]
                    if target not in rec:
                        sc_prop_type = sc_prop.get("type", "string")
                        if sc_prop_type == "array":
                            prop[target] = []
                            break
                            # prop[target].append({})
                        elif sc_parent["type"] == "array" and sc_prop_type == "string":
                            print(f"break! {target} {sc_prop} {sc_parent} {prop}")
                            break
                        else:
                            if sc_parent["type"] == "array":
                                prop[target] = {}
                                parent.append(prop[target])
                            else:
                                raise Exception(
                                    f"Edge! {target_string} {sch[target_string]}"
                                )
                    if target == targets[-1]:
                        prop[target] = value[0]
                    prop = prop[target]
                    sc_parent = sc_prop
                    parent = target

    def validate(self, folio_rec):
        if folio_rec["title"].strip() == "":
            raise ValueError(f"No title for {folio_rec['hrid']}")
        for key, value in folio_rec.items():
            if isinstance(value, str) and any(value):
                self.mapped_folio_fields["key]"] = (
                    self.mapped_folio_fields.get(key, 0) + 1
                )
            if isinstance(value, list) and any(value):
                self.mapped_folio_fields["key]"] = (
                    self.mapped_folio_fields.get(key, 0) + 1
                )

    def save_source_record(self, marc_record, instance_id):
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
            (marc_record, instance_id, srs_id, self.folio.get_metadata_construct())
        )
        self.marc_xml_writer.write(marc_record)
        if len(self.srs_recs) == 1000:
            self.flush_srs_recs()
            self.srs_recs = []

    def flush_srs_recs(self):
        pool = ProcessPoolExecutor(max_workers=4)
        results = list(pool.map(get_srs_strings, self.srs_recs))
        """for srs_rec in self.srs_recs:
            r = get_srs_strings(srs_rec)
            self.srs_records_file.write(r[0])
            self.srs_marc_records_file.write(r[2])
            self.srs_raw_records_file.write(r[1])"""
        self.srs_records_file.write("".join(r[0] for r in results))
        self.srs_marc_records_file.write("".join(r[2] for r in results))
        self.srs_raw_records_file.write("".join(r[1] for r in results))

    def get_nature_of_content(self, marc_record):
        return ["81a3a0e2-b8e5-4a7a-875d-343035b4e4d7"]

    def get_mode_of_issuance_id(self, marc_record):
        try:
            seventh = marc_record.leader[7]
            m_o_i_s = {
                "m": "Monograph",
                "s": "Serial",
                "i": "Integrating Resource",
            }
            name = m_o_i_s.get(seventh, "Other")
            if not name:
                raise Exception(f"{name} is not a valid mode of issuance")
            return next(
                i["id"] for i in self.folio.modes_of_issuance if name == i["name"]
            )
        except IndexError:
            raise ValueError(f"No seven in {marc_record.leader}")

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
                    print(
                        f"Illegal language code: {language_value} for {get_legacy_id(marc_record)}"
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


def get_srs_strings(my_tuple):
    json_string = StringIO()
    writer = JSONWriter(json_string)
    writer.write(my_tuple[0])
    writer.close(close_fh=False)
    marc_uuid = str(uuid.uuid4())
    raw_uuid = str(uuid.uuid4())
    record = {
        "id": my_tuple[2],
        "deleted": False,
        "snapshotId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
        "matchedProfileId": str(uuid.uuid4()),
        "matchedId": str(uuid.uuid4()),
        "generation": 1,
        "recordType": "MARC",
        "rawRecordId": raw_uuid,
        "parsedRecordId": marc_uuid,
        "additionalInfo": {"suppressDiscovery": False},
        "externalIdsHolder": {"instanceId": my_tuple[1]},
        "metadata": my_tuple[3],
    }
    raw_record = {"id": raw_uuid, "content": my_tuple[0].as_json()}
    marc_record = {"id": marc_uuid, "content": json.loads(my_tuple[0].as_json())}
    return (
        f"{record['id']}\t{json.dumps(record)}\n",
        f"{raw_record['id']}\t{json.dumps(raw_record)}\n",
        f"{marc_record['id']}\t{json.dumps(marc_record)}\n",
    )


def grouped(iterable, n):
    "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
    return zip(*[iter(iterable)] * n)


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def get_legacy_id(marc_record):
    if "001" in marc_record and marc_record["001"]:
        return marc_record["001"].format_field()
    elif "907" in marc_record and "a" in marc_record["907"] and marc_record["907"]["a"]:
        return marc_record["907"]["a"]
    else:
        return marc_record.title()


def has_conditions(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("conditions", [])


def has_value_to_add(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("value", "")


def get_source_id(marc_record):
    """Gets the system Id from sierra"""
    if "907" in marc_record and "a" in marc_record["907"]:
        return marc_record["907"]["a"].replace(".b", "")[:-1]
    elif "001" in marc_record:
        return marc_record["001"].format_field()
    else:
        raise ValueError("No 001 present")
