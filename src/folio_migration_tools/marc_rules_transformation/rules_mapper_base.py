import datetime
import json
import logging
import time
import uuid
from textwrap import wrap

import pymarc
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from folioclient import FolioClient
from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.report_blurbs import Blurbs
from pymarc import Field, Record, Leader


class RulesMapperBase(MapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        conditions=None,
    ):
        super().__init__(library_configuration, folio_client)
        self.parsed_records = 0
        self.start = time.time()
        self.last_batch_time = time.time()
        self.folio_client: FolioClient = folio_client
        self.holdings_json_schema = self.fetch_holdings_schema()
        self.instance_json_schema = self.get_instance_schema()
        self.schema = {}
        self.conditions = conditions
        self.item_json_schema = ""
        self.mappings = {}
        self.schema_properties = None
        logging.info("Current user id is %s", self.folio_client.current_user)

    # TODO: Rebuild and move
    def print_progress(self):
        self.parsed_records += 1
        num_recs = 5000
        if self.parsed_records % num_recs == 0:
            elapsed = self.parsed_records / (time.time() - self.start)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            elapsed_last = num_recs / (time.time() - self.last_batch_time)
            elapsed_formatted_last = "{0:.4g}".format(elapsed_last)
            logging.info(
                f"{elapsed_formatted_last} (avg. {elapsed_formatted}) "
                f"records/sec.\t\t{self.parsed_records:,} records processed"
            )
            self.last_batch_time = time.time()

    @staticmethod
    def dedupe_rec(rec):
        # remove duplicates
        for key, value in rec.items():
            if isinstance(value, list):
                res = []
                for v in value:
                    if v not in res:
                        res.append(v)
                rec[key] = list(res)

    def map_field_according_to_mapping(
        self, marc_field: pymarc.Field, mappings, folio_record, legacy_ids
    ):
        for mapping in mappings:
            if "entity" not in mapping:
                self.handle_normal_mapping(
                    mapping, marc_field, folio_record, legacy_ids
                )
            else:
                self.handle_entity_mapping(
                    marc_field,
                    mapping,
                    folio_record,
                    legacy_ids,
                )

    def handle_normal_mapping(
        self, mapping, marc_field: pymarc.Field, folio_record, legacy_ids
    ):
        target = mapping["target"]
        if mapping.get("ignoreSubsequentSubfields", False):
            marc_field = self.remove_repeated_subfields(marc_field)
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

    @staticmethod
    def set_005_as_updated_date(marc_record: Record, folio_object: dict, legacy_ids):
        try:
            f005 = marc_record["005"].data[:14]
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
            first_six = "".join(marc_record["008"].data[:6])
            date_str = f"19{first_six}" if int(first_six[:2]) > 69 else f"20{first_six}"
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
        legacy_id,
        mapping,
        marc_field,
    ):
        stripped_conds = mapping["rules"][0]["conditions"][0]["type"].split(",")
        condition_types = list(map(str.strip, stripped_conds))
        parameter = mapping["rules"][0]["conditions"][0].get("parameter", {})
        if mapping.get("applyRulesOnConcatenatedData", ""):
            value = " ".join(marc_field.get_subfields(*mapping["subfield"]))
            return self.apply_rule(
                legacy_id, value, condition_types, marc_field, parameter
            )
        elif mapping.get("subfield", []):
            subfields = marc_field.get_subfields(*mapping["subfield"])
            x = [
                self.apply_rule(legacy_id, x, condition_types, marc_field, parameter)
                for x in subfields
            ]
            return " ".join(set(x))
        else:
            value1 = marc_field.format_field() if marc_field else ""
            return self.apply_rule(
                legacy_id, value1, condition_types, marc_field, parameter
            )

    def apply_rules(self, marc_field: pymarc.Field, mapping, legacy_ids):
        try:
            values = []
            value = ""
            if has_conditions(mapping):
                value = self.get_value_from_condition(
                    ",".join(legacy_ids), mapping, marc_field
                )
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
            trfe.log_it()
            self.migration_report.add_general_statistics(
                "Records failed due to an error. See data issues log for details"
            )
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
                        raise TransformationProcessError("", s)
                        # break
                    else:
                        if schema_parent["type"] == "array":
                            parent.append({})
                        else:
                            raise TransformationProcessError(
                                "",
                                f"Edge! Something in the schemas has changed. "
                                "The mapping of this needs to be investigated "
                                f"{target_string} {schema_properties[target_string]}",
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
                "",
                f"Target string {target_string} not in Schema! Check mapping file against the schema."
                f"Target type: {sch.get(target_string,{}).get('type','')} Value: {value}",
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
                "",
                f"Edge! Target string: {target_string} Target type: {sch.get(target_string,{}).get('type','')} Value: {value}",
            )

    def create_entity(
        self, entity_mappings, marc_field, entity_parent_key, index_or_legacy_id
    ):
        entity = {}
        for entity_mapping in entity_mappings:
            k = entity_mapping["target"].split(".")[-1]
            if values := self.apply_rules(
                marc_field, entity_mapping, index_or_legacy_id
            ):
                if entity_parent_key == k:
                    entity = values[0]
                else:
                    entity[k] = values[0]
        return entity

    def handle_entity_mapping(
        self,
        marc_field,
        mapping,
        folio_record,
        legacy_ids,
    ):
        entity_mapping = mapping["entity"]
        e_parent = entity_mapping[0]["target"].split(".")[0]
        if mapping.get("entityPerRepeatedSubfield", False):
            for temp_field in self.grouped(marc_field):
                entity = self.create_entity(
                    entity_mapping, temp_field, e_parent, legacy_ids
                )
                if (type(entity) is dict and all(entity.values())) or (
                    type(entity) is list and all(entity)
                ):
                    self.add_entity_to_record(
                        entity, e_parent, folio_record, self.schema
                    )
        else:
            if mapping.get("ignoreSubsequentSubfields", False):
                marc_field = self.remove_repeated_subfields(marc_field)
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
                self.add_entity_to_record(entity, e_parent, folio_record, self.schema)
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
                # self.add_entity_to_record(entity, e_parent, rec, self.schema)

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

    def apply_rule(self, legacy_id, value, condition_types, marc_field, parameter):
        v = value
        for condition_type in iter(condition_types):
            v = self.conditions.get_condition(
                condition_type, legacy_id, v, parameter, marc_field
            )
        return v

    @staticmethod
    def add_entity_to_record(entity, entity_parent_key, rec, schema):
        sch = schema["properties"]
        if sch[entity_parent_key]["type"] == "array":
            if entity_parent_key not in rec:
                rec[entity_parent_key] = [entity]
            else:
                rec[entity_parent_key].append(entity)
        else:
            rec[entity_parent_key] = entity

    @staticmethod
    def get_instance_schema():
        logging.info("Fetching Instance schema...")
        instance_schema = FolioClient.get_latest_from_github(
            "folio-org", "mod-inventory-storage", "ramls/instance.json"
        )
        logging.info("done")
        return instance_schema

    @staticmethod
    def fetch_holdings_schema():
        logging.info("Fetching HoldingsRecord schema...")
        holdings_record_schema = FolioClient.get_latest_from_github(
            "folio-org", "mod-inventory-storage", "ramls/holdingsrecord.json"
        )
        logging.info("done")
        return holdings_record_schema

    @staticmethod
    def grouped(marc_field: Field):
        "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
        unique_subfields = []
        repeated_subfields = []
        results = list()
        for sf, sf_vals in marc_field.subfields_as_dict().items():
            if len(sf_vals) == 1:
                unique_subfields.extend([sf, sf_vals[0]])
            else:
                for sf_val in sf_vals:
                    repeated_subfields.append([sf, sf_val])
        if any(repeated_subfields):
            for repeated_subfield in repeated_subfields:
                new_subfields = [repeated_subfield[0], repeated_subfield[1]]
                new_subfields.extend(unique_subfields)
                temp_field = Field(
                    tag=marc_field.tag,
                    indicators=marc_field.indicators,
                    subfields=new_subfields,
                )
                results.append(temp_field)
        else:
            temp_field = Field(
                tag=marc_field.tag,
                indicators=marc_field.indicators,
                subfields=unique_subfields,
            )
            results.append(temp_field)
        return results

    @staticmethod
    def remove_repeated_subfields(marc_field: Field):
        "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
        new_subfields = []
        for sf, sf_vals in marc_field.subfields_as_dict().items():
            new_subfields.extend([sf, sf_vals[0]])
        return Field(
            tag=marc_field.tag,
            indicators=marc_field.indicators,
            subfields=new_subfields,
        )

    @staticmethod
    def save_source_record(
        srs_records_file,
        record_type: FOLIONamespaces,
        folio_client: FolioClient,
        marc_record: Record,
        folio_record,
        legacy_id: str,
        suppress: bool,
    ):
        """Saves the source Marc_record to the Source record Storage module"""
        srs_id = RulesMapperBase.create_srs_id(
            record_type, folio_client.okapi_url, legacy_id
        )

        marc_record.add_ordered_field(
            Field(
                tag="999",
                indicators=["f", "f"],
                subfields=["i", folio_record["id"], "s", srs_id],
            )
        )
        # Since they all should be UTF encoded, make the leader align.
        try:
            temp_leader = Leader(marc_record.leader)
            temp_leader[9] = "a"
            marc_record.leader = temp_leader
        except Exception:
            logging.exception(
                "Something is wrong with the marc records leader: %s",
                marc_record.leader,
            )
        srs_record_string = RulesMapperBase.get_srs_string(
            marc_record,
            folio_record,
            srs_id,
            folio_client.get_metadata_construct(),
            suppress,
            record_type,
        )
        srs_records_file.write(f"{srs_record_string}\n")

    @staticmethod
    def create_srs_id(record_type, okapi_url: str, legacy_id: str):
        srs_types = {
            FOLIONamespaces.holdings: FOLIONamespaces.srs_records_holdingsrecord,
            FOLIONamespaces.instances: FOLIONamespaces.srs_records_bib,
            FOLIONamespaces.athorities: FOLIONamespaces.srs_records_auth,
            FOLIONamespaces.edifact: FOLIONamespaces.srs_records_edifact,
        }

        return str(
            FolioUUID(
                okapi_url,
                srs_types.get(record_type),
                str(legacy_id),
            )
        )

    @staticmethod
    def get_srs_string(
        marc_record: Record,
        folio_object: dict,
        srs_id,
        metadata_obj,
        suppress,
        record_type: FOLIONamespaces,
    ):
        record_types = {
            FOLIONamespaces.holdings: "MARC_HOLDING",
            FOLIONamespaces.instances: "MARC_BIB",
            FOLIONamespaces.athorities: "MARC_AUTHORITY",
            FOLIONamespaces.edifact: "EDIFACT",
        }

        id_holders = {
            FOLIONamespaces.instances: {
                "instanceId": folio_object["id"],
                "instanceHrid": folio_object["hrid"],
            },
            FOLIONamespaces.holdings: {
                "holdingsId": folio_object["id"],
                "holdingsHrid": folio_object["hrid"],
            },
            FOLIONamespaces.athorities: {},
            FOLIONamespaces.edifact: {},
        }

        my_tuple_json = marc_record.as_json()
        raw_record = {"id": srs_id, "content": my_tuple_json}
        parsed_record = {"id": srs_id, "content": json.loads(my_tuple_json)}
        record = {
            "id": srs_id,
            "deleted": False,
            "matchedId": srs_id,
            "generation": 0,
            "recordType": record_types.get(record_type),
            "rawRecord": raw_record,
            "parsedRecord": parsed_record,
            "additionalInfo": {"suppressDiscovery": suppress},
            "externalIdsHolder": id_holders.get(record_type),
            "metadata": metadata_obj,
            "state": "ACTUAL",
            "leaderRecordStatus": parsed_record["content"]["leader"][5]
            if parsed_record["content"]["leader"][5] in [*"acdnposx"]
            else "d",
        }
        return json.dumps(record)


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
