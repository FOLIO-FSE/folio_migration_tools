import json
import logging
import time
import uuid
from textwrap import wrap

import pymarc
from folioclient import FolioClient
from migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
import datetime
from migration_tools.helper import Helper
from migration_tools.mapper_base import MapperBase
from migration_tools.report_blurbs import Blurbs
from pymarc import Field, Record


class RulesMapperBase(MapperBase):
    def __init__(self, folio_client: FolioClient, conditions=None):
        super().__init__()
        self.parsed_records = 0
        self.start = time.time()
        self.folio_client: FolioClient = folio_client
        self.holdings_json_schema = fetch_holdings_schema()
        self.instance_json_schema = get_instance_schema()
        self.schema = {}
        self.conditions = conditions
        self.item_json_schema = ""
        self.mappings = {}
        self.schema_properties = None
        logging.info("Current user id is %s", self.folio_client.current_user)

    def print_progress(self):
        self.parsed_records += 1
        if self.parsed_records % 5000 == 0:
            elapsed = self.parsed_records / (time.time() - self.start)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            logging.info(
                f"{elapsed_formatted} records/sec.\t\t{self.parsed_records:,} records processed"
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

    def map_field_according_to_mapping(
        self, marc_field: pymarc.Field, mappings, folio_record, legacy_ids
    ):
        for mapping in mappings:
            if "entity" not in mapping:
                target = mapping["target"]
                if has_conditions(mapping):
                    values = self.apply_rules(marc_field, mapping, legacy_ids)
                    # TODO: add condition to customize this hardcoded thing
                    if marc_field.tag == "655":
                        values[0] = f"Genre: {values[0]}"
                    self.add_value_to_target(folio_record, target, values)
                elif has_value_to_add(mapping):
                    value = mapping["rules"][0]["value"]
                    # Stupid construct to avoid bool("false") == True
                    if value == "true":
                        self.add_value_to_target(folio_record, target, [True])
                    elif value == "false":
                        self.add_value_to_target(folio_record, target, [False])
                    else:
                        self.add_value_to_target(folio_record, target, [value])
                else:
                    # Adding stuff without rules/Conditions.
                    # Might need more complex mapping for arrays etc
                    if any(mapping["subfield"]):
                        value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
                    else:
                        value = marc_field.format_field() if marc_field else ""
                    self.add_value_to_target(folio_record, target, [value])
            else:
                e_per_subfield = mapping.get("entityPerRepeatedSubfield", False)
                self.handle_entity_mapping(
                    marc_field,
                    mapping["entity"],
                    folio_record,
                    e_per_subfield,
                    legacy_ids,
                )

    @staticmethod
    def set_005_as_updated_date(marc_record: Record, folio_object: dict, legacy_ids):
        try:
            f005 = marc_record["005"].data[0:14]
            parsed_date = datetime.datetime.strptime(f005, "%Y%m%d%H%M%S").isoformat()
            folio_object["metadata"]["updatedDate"] = parsed_date
        except Exception as exception:
            if "005" in marc_record:
                Helper.log_data_issue(
                    legacy_ids,
                    f"Could not parse Last transaction date from 005 {exception}",
                    marc_record["005"].data,
                )

    @staticmethod
    def use_008_for_dates(marc_record: Record, folio_object: dict, legacy_ids):
        try:
            first_six = "".join(marc_record["008"].data[0:6])
            date_str = (
                f"19{first_six}" if int(first_six[0:2]) > 69 else f"20{first_six}"
            )
            date_str_parsed = datetime.datetime.strptime(date_str, "%Y%m%d")
            if "title" in folio_object:  # only instance has titles
                folio_object["catalogedDate"] = date_str_parsed.strftime("%Y-%m-%d")
        except Exception as exception:
            if "008" in marc_record:
                Helper.log_data_issue(
                    legacy_ids,
                    f"Could not parse cat date from 008 {exception}",
                    marc_record["008"].data,
                )

    def get_value_from_condition(
        self,
        mapping,
        marc_field,
    ):
        condition_types = [
            x.strip() for x in mapping["rules"][0]["conditions"][0]["type"].split(",")
        ]
        parameter = mapping["rules"][0]["conditions"][0].get("parameter", {})
        if mapping.get("applyRulesOnConcatenatedData", ""):
            value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
            return self.apply_rule(value, condition_types, marc_field, parameter)
        elif mapping.get("subfield", []):
            if mapping.get("ignoreSubsequentFields", False):
                subfields = []
                for sf in mapping["subfield"]:
                    next_subfield = next(iter(marc_field.get_subfields(sf)), "")
                    subfields.append(next_subfield)
                return " ".join(
                    self.apply_rule(x, condition_types, marc_field, parameter)
                    for x in subfields
                )
            else:
                subfields = marc_field.get_subfields(*mapping["subfield"])
                x = [
                    self.apply_rule(x, condition_types, marc_field, parameter)
                    for x in subfields
                ]
                return " ".join(set(x))
        else:
            value1 = marc_field.format_field() if marc_field else ""
            return self.apply_rule(value1, condition_types, marc_field, parameter)

    def apply_rules(self, marc_field: pymarc.Field, mapping, legacy_ids):
        try:
            values = []
            value = ""
            if has_conditions(mapping):
                value = self.get_value_from_condition(mapping, marc_field)
            elif has_value_to_add(mapping):
                value = mapping["rules"][0]["value"]
                if value == "false":
                    return [False]
                elif value == "true":
                    return [True]
                else:
                    return [value]
            elif not mapping.get("rules", []) or not mapping["rules"][0].get(
                "conditions", []
            ):
                value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
            values = wrap(value, 3) if mapping.get("subFieldSplit", "") else [value]
            return values
        except TransformationProcessError as trpe:
            self.handle_transformation_process_error(self.parsed_records, trpe)
        except TransformationFieldMappingError as fme:
            self.migration_report.add(Blurbs.FieldMappingErrors, fme.message)
            fme.data_value = f"{fme.data_value} MARCField: {marc_field} Mapping: {json.dumps(mapping)}"
            fme.log_it()
            return []
        except TransformationRecordFailedError as trfe:
            trfe.data_value = (
                f"{trfe.data_value} MARCField: {marc_field} "
                f"Mapping: {json.dumps(mapping)}"
            )
            raise trfe
        except Exception as exception:
            self.handle_generic_exception(self.parsed_records, exception)

    def add_value_to_target(self, rec, target_string, value):
        if not value:
            return
        targets = target_string.split(".")
        if len(targets) == 1:
            self.add_value_to_first_level_target(rec, target_string, value)

        else:
            schema_parent = None
            parent = None
            schema_properties = self.schema["properties"]
            sc_prop = schema_properties
            for target in targets:  # Iterate over names in hierarcy
                if target in sc_prop:  # property is on this level
                    sc_prop = sc_prop[target]  # set current property
                else:  # next level. take the properties from the items
                    sc_prop = schema_parent["items"]["properties"][target]
                if (
                    target not in rec and not schema_parent
                ):  # have we added this already?
                    if is_array_of_strings(sc_prop):
                        rec[target] = []
                        # break
                        # prop[target].append({})
                    elif is_array_of_objects(sc_prop):
                        rec[target] = [{}]
                        # break
                    elif (
                        schema_parent
                        and is_array_of_objects(schema_parent)
                        and sc_prop.get("type", "string") == "string"
                    ):
                        s = "This should be unreachable code. Check schema for changes"
                        logging.error(s)
                        logging.error(parent)
                        raise TransformationProcessError(s)
                        # break
                    else:
                        if schema_parent["type"] == "array":
                            parent.append({})
                        else:
                            raise TransformationProcessError(
                                f"Edge! Something in the schemas has changed. "
                                "The mapping of this needs to be investigated "
                                f"{target_string} {schema_properties[target_string]}"
                            )
                elif is_array_of_objects(sc_prop) and len(rec[target][-1]) == len(
                    sc_prop["items"]["properties"]
                ):
                    rec[target].append({})
                elif schema_parent and target in rec[parent][-1]:
                    rec[parent].append({})
                    if len(rec[parent][-1]) > 0:
                        rec[parent][-1][target] = value[0]
                    else:
                        rec[parent][-1] = {target: value[0]}
                elif (
                    schema_parent
                    and is_array_of_objects(schema_parent)
                    and sc_prop.get("type", "string") == "string"
                ):
                    if len(rec[parent][-1]) > 0:
                        rec[parent][-1][target] = value[0]
                    else:
                        rec[parent][-1] = {target: value[0]}
                # if target == targets[-1]:
                # prop[target] = value[0]
                # prop = rec[target]
                schema_parent = sc_prop
                parent = target

    def add_value_to_first_level_target(self, rec, target_string, value):
        sch = self.schema["properties"]

        if not target_string or target_string not in sch:
            raise TransformationProcessError(
                f"Target string {target_string} not in Schema! Check mapping file against the schema."
                f"Target type: {sch.get(target_string,{}).get('type','')} Value: {value}"
            )

        target_field = sch.get(target_string, {})
        if (
            target_field.get("type", "") == "array"
            and target_field.get("items", {}).get("type", "") == "string"
        ):

            if target_string not in rec:
                rec[target_string] = value
            else:
                rec[target_string].extend(value)

        elif target_field.get("type", "") == "string":
            if value[0]:
                rec[target_string] = value[0]
        else:
            raise TransformationProcessError(
                f"Edge! Target string: {target_string} Target type: {sch.get(target_string,{}).get('type','')} Value: {value}"
            )

    def create_entity(
        self, entity_mappings, marc_field, entity_parent_key, index_or_legacy_id
    ):
        entity = {}
        for entity_mapping in entity_mappings:
            k = entity_mapping["target"].split(".")[-1]
            values = self.apply_rules(marc_field, entity_mapping, index_or_legacy_id)
            if values:
                if entity_parent_key == k:
                    entity = values[0]
                else:
                    entity[k] = values[0]
        return entity

    def handle_entity_mapping(
        self,
        marc_field,
        entity_mapping,
        folio_record,
        e_per_subfield,
        legacy_ids,
    ):
        e_parent = entity_mapping[0]["target"].split(".")[0]
        if e_per_subfield:
            for sf_tuple in grouped(marc_field.subfields, 2):
                temp_field = Field(
                    tag=marc_field.tag,
                    indicators=marc_field.indicators,
                    subfields=[sf_tuple[0], sf_tuple[1]],
                )
                entity = self.create_entity(
                    entity_mapping, temp_field, e_parent, legacy_ids
                )
                if (type(entity) is dict and any(entity.values())) or (
                    type(entity) is list and any(entity)
                ):
                    self.add_entity_to_record(entity, e_parent, folio_record)
        else:
            entity = self.create_entity(
                entity_mapping, marc_field, e_parent, legacy_ids
            )
            if e_parent in ["precedingTitles", "succeedingTitles"]:
                self.create_preceding_succeeding_titles(
                    entity, e_parent, folio_record["id"]
                )
            elif (
                all(
                    v
                    for k, v in entity.items()
                    if k
                    not in [
                        "staffOnly",
                        "primary",
                        "isbnValue",
                        "issnValue",
                    ]
                )
                or e_parent in ["electronicAccess", "publication"]
                or (
                    e_parent.startswith("holdingsStatements")
                    and any(v for k, v in entity.items())
                )
            ):
                self.add_entity_to_record(entity, e_parent, folio_record)
            else:
                sfs = " - ".join(
                    f"{f[0]}:{('has_value' if f[1].strip() else 'empty')}"
                    for f in marc_field
                )
                pattern = " - ".join(f"{k}:'{bool(v)}'" for k, v in entity.items())
                self.migration_report.add(
                    Blurbs.IncompleteEntityMapping,
                    f"{marc_field.tag} {sfs} ->>-->> {e_parent} {pattern}  ",
                )
                # Experimental
                # self.add_entity_to_record(entity, e_parent, rec)

    def create_preceding_succeeding_titles(self, entity, e_parent, identifier):
        self.migration_report.add(
            Blurbs.PrecedingSuccedingTitles, f"{e_parent} created"
        )
        # TODO: Make these uuids deterministic
        new_entity = {
            "id": str(uuid.uuid4()),
            "title": entity.get("title"),
            "identifiers": [],
        }
        if e_parent == "precedingTitles":
            new_entity["succeedingInstanceId"] = identifier
        else:
            new_entity["precedingInstanceId"] = identifier
        if new_entity.get("isbnValue", ""):
            new_entity["identifiers"].append(
                {
                    "identifierTypeId": new_entity.get("isbnId"),
                    "value": new_entity.get("isbnValue"),
                }
            )
        if new_entity.get("issnValue", ""):
            new_entity["identifiers"].append(
                {
                    "identifierTypeId": new_entity.get("issnId"),
                    "value": new_entity.get("issnValue"),
                }
            )
        logging.log(25, f"{e_parent}\t{json.dumps(new_entity)}")

    def apply_rule(self, value, condition_types, marc_field, parameter):
        v = value
        for condition_type in condition_types:
            v = self.conditions.get_condition(condition_type, v, parameter, marc_field)
        return v

    def add_entity_to_record(self, entity, entity_parent_key, rec):
        sch = self.schema["properties"]
        if sch[entity_parent_key]["type"] == "array":
            if entity_parent_key not in rec:
                rec[entity_parent_key] = [entity]
            else:
                rec[entity_parent_key].append(entity)
        else:
            rec[entity_parent_key] = entity


def fetch_holdings_schema():
    logging.info("Fetching HoldingsRecord schema...")
    holdings_record_schema = Helper.get_latest_from_github(
        "folio-org", "mod-inventory-storage", "ramls/holdingsrecord.json"
    )
    logging.info("done")
    return holdings_record_schema


def get_instance_schema():
    logging.info("Fetching Instance schema...")
    instance_schema = Helper.get_latest_from_github(
        "folio-org", "mod-inventory-storage", "ramls/instance.json"
    )
    logging.info("done")
    return instance_schema


def has_conditions(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("conditions", [])


def has_value_to_add(mapping):
    return mapping.get("rules", []) and mapping["rules"][0].get("value", "")


def is_array_of_strings(schema_property):
    sc_prop_type = schema_property.get("type", "string")
    return sc_prop_type == "array" and schema_property["items"]["type"] == "string"


def is_array_of_objects(schema_property):
    sc_prop_type = schema_property.get("type", "string")
    return sc_prop_type == "array" and schema_property["items"]["type"] == "object"


def grouped(iterable, n):
    "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
    return zip(*[iter(iterable)] * n)
