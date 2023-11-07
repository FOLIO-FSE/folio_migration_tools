import datetime
import json
import logging
import time
import urllib.parse
import uuid
from abc import abstractmethod
from textwrap import wrap
from typing import List
from typing import Tuple

import i18n
import pymarc
from dateutil.parser import parse
from folio_uuid.folio_uuid import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient
from pymarc import Field
from pymarc import Leader
from pymarc import Record
from pymarc import Subfield

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler


class RulesMapperBase(MapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        task_configuration,
        schema: dict,
        conditions=None,
        parent_id_map: dict[str, tuple] = None,
    ):
        super().__init__(library_configuration, folio_client, parent_id_map)
        self.parsed_records = 0
        self.id_map: dict[str, tuple] = {}
        self.start = time.time()
        self.last_batch_time = time.time()
        self.folio_client: FolioClient = folio_client
        self.schema: dict = schema
        self.task_configuration = task_configuration
        self.conditions = conditions
        self.item_json_schema = ""
        self.mappings: dict = {}
        self.schema_properties = None
        if hasattr(self.task_configuration, "hrid_handling"):
            self.hrid_handler = HRIDHandler(
                folio_client,
                self.task_configuration.hrid_handling,
                self.migration_report,
                self.task_configuration.deactivate035_from001,
            )
        logging.info("Current user id is %s", self.folio_client.current_user)

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

    @abstractmethod
    def get_legacy_ids(self, marc_record: Record, idx: int):
        raise NotImplementedError()

    @staticmethod
    def dedupe_rec(rec, props_to_not_dedupe=None):
        if props_to_not_dedupe is None:
            props_to_not_dedupe = []
        # remove duplicates
        for key, value in rec.items():
            if key not in props_to_not_dedupe and isinstance(value, list):
                res = []
                for v in value:
                    if v not in res:
                        res.append(v)
                rec[key] = list(res)

    def map_field_according_to_mapping(
        self, marc_field: pymarc.Field, mappings, folio_record, legacy_ids
    ):
        for mapping in mappings:
            try:
                if "entity" not in mapping:
                    self.handle_normal_mapping(mapping, marc_field, folio_record, legacy_ids)
                else:
                    self.handle_entity_mapping(
                        marc_field,
                        mapping,
                        folio_record,
                        legacy_ids,
                    )
            except TransformationFieldMappingError as tre:
                tre.log_it()

    def handle_normal_mapping(self, mapping, marc_field: pymarc.Field, folio_record, legacy_ids):
        target = mapping["target"]
        if mapping.get("ignoreSubsequentSubfields", False):
            marc_field = self.remove_repeated_subfields(marc_field)
        if has_conditions(mapping):
            values = self.apply_rules(marc_field, mapping, legacy_ids)
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
                values = self.handle_sub_field_delimiters(
                    ",".join(legacy_ids), mapping, marc_field
                )
                value = " ".join(values)
            else:
                value = marc_field.format_field() if marc_field else ""
            self.add_value_to_target(folio_record, target, [value])

    @staticmethod
    def set_005_as_updated_date(marc_record: Record, folio_object: dict, legacy_ids):
        try:
            f005 = marc_record["005"].data[:14]
            parsed_date = datetime.datetime.strptime(f005, "%Y%m%d%H%M%S").isoformat()
        except Exception as exception:
            if "005" in marc_record:
                Helper.log_data_issue(
                    legacy_ids,
                    f"Could not parse Last transaction date from 005 {exception}",
                    marc_record["005"].data,
                )

    @abstractmethod
    def parse_record(
        self, marc_record: Record, file_def: FileDefinition, legacy_ids: List[str]
    ) -> list[dict]:
        raise NotImplementedError()

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

    def handle_sub_field_delimiters(
        self,
        legacy_id: str,
        mapping,
        marc_field: pymarc.Field,
        condition_types: List[str] = None,
        parameter: dict = None,
    ):
        values: List[str] = []
        if mapping.get("subfield") and (custom_delimiters := mapping.get("subFieldDelimiter")):
            delimiter_map = {sub_f: " " for sub_f in mapping.get("subfield")}
            for custom_delimiter in custom_delimiters:
                delimiter_map.update(
                    {sub_f: custom_delimiter["value"] for sub_f in custom_delimiter["subfields"]}
                )
            custom_delimited_strings: List[Tuple[str, List[str]]] = []
            subfields = mapping.get("subfield")
            for custom_delimiter in custom_delimiters:
                subfields_for_delimiter = [
                    sub_f
                    for sub_f in subfields
                    if custom_delimiter["subfields"]
                    and delimiter_map[sub_f] == custom_delimiter["value"]
                ]
                subfield_collection: Tuple[str, List[str]] = (custom_delimiter["value"], [])
                subfield_collection[1].extend(marc_field.get_subfields(*subfields_for_delimiter))
                custom_delimited_strings.append(subfield_collection)
            for custom_delimited_string in custom_delimited_strings:
                if mapping.get("applyRulesOnConcatenatedData", ""):
                    values.extend(custom_delimited_string[1])
                else:
                    values.extend(
                        dict.fromkeys(
                            [
                                self.apply_rule(
                                    legacy_id,
                                    x,
                                    condition_types or [],
                                    marc_field,
                                    parameter or {},
                                )
                                for x in custom_delimited_string[1]
                            ]
                        )
                    )
                values = [custom_delimited_string[0].join(values)]
        elif mapping.get("subfield", []):
            values.extend(marc_field.get_subfields(*mapping["subfield"]))
        return values

    def get_value_from_condition(
        self,
        legacy_id,
        mapping,
        marc_field,
    ):
        stripped_conds = mapping["rules"][0]["conditions"][0]["type"].split(",")
        condition_types = list(map(str.strip, stripped_conds))
        parameter = mapping["rules"][0]["conditions"][0].get("parameter", {})
        values: List[str] = []
        if mapping.get("subfield"):
            values.extend(
                self.handle_sub_field_delimiters(
                    legacy_id, mapping, marc_field, condition_types, parameter
                )
            )
        else:
            values.append(marc_field.format_field() if marc_field else "")

        if not mapping.get("applyRulesOnConcatenatedData", "") and mapping.get("subfield", []):
            return " ".join(
                dict.fromkeys(
                    [
                        self.apply_rule(legacy_id, x, condition_types, marc_field, parameter)
                        for x in values
                    ]
                )
            )
        else:
            return self.apply_rule(
                legacy_id, " ".join(values), condition_types, marc_field, parameter
            )

    def process_marc_field(
        self,
        folio_record: dict,
        marc_field: Field,
        ignored_subsequent_fields,
        legacy_ids,
    ):
        if marc_field.tag == "880":
            mappings = self.perform_proxy_mapping(marc_field)
        else:
            tags_to_ignore = {"880", "001", "008"}
            mappings = (
                self.mappings.get(marc_field.tag, {})
                if marc_field.tag not in tags_to_ignore
                else []
            )
        if mappings:
            try:
                self.map_field_according_to_mapping(marc_field, mappings, folio_record, legacy_ids)
                if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                    ignored_subsequent_fields.add(marc_field.tag)
            except Exception as ee:
                logging.error(
                    "map_field_according_to_mapping %s %s %s",
                    marc_field.tag,
                    marc_field.format_field(),
                    json.dumps(mappings),
                )
                raise ee

    def perform_proxy_mapping(self, marc_field):
        proxy_mapping = next(iter(self.mappings.get("880", [])), [])
        if "6" not in marc_field:
            self.migration_report.add("Field880Mappings", i18n.t("Records without $6"))
            return None
        if not proxy_mapping or not proxy_mapping.get("fieldReplacementBy3Digits", False):
            return None
        if not marc_field["6"][:3] or len(marc_field["6"][:3]) != 3:
            self.migration_report.add(
                "Field880Mappings", i18n.t("Records with unexpected length in $6")
            )
            return None
        first_three = marc_field["6"][:3]

        target_field = next(
            (
                r.get("targetField", "")
                for r in proxy_mapping.get("fieldReplacementRule", [])
                if r["sourceDigits"] == first_three
            ),
            first_three,
        )
        self.migration_report.add(
            "Field880Mappings",
            i18n.t("Source digits")
            + f": {marc_field['6']} "
            + i18n.t("Target field")
            + f": {target_field}",
        )
        mappings = self.mappings.get(target_field, {})
        if not mappings:
            self.migration_report.add(
                "Field880Mappings",
                i18n.t("Mapping not set up for target field")
                + f": {target_field} ({marc_field['6']})",
            )
        return mappings

    def report_marc_stats(
        self, marc_field: Field, bad_tags, legacy_ids, ignored_subsequent_fields
    ):
        self.migration_report.add("Trivia", i18n.t("Total number of Tags processed"))
        self.report_source_and_links(marc_field)
        self.report_bad_tags(marc_field, bad_tags, legacy_ids)
        mapped = marc_field.tag in self.mappings
        if marc_field.tag in ignored_subsequent_fields:
            mapped = False
        self.report_legacy_mapping(marc_field.tag, True, mapped)

    def report_source_and_links(self, marc_field: Field):
        if marc_field.is_control_field():
            return
        for subfield_2 in marc_field.get_subfields("2"):
            self.migration_report.add(
                "AuthoritySources",
                i18n.t("Source of heading or term") + f": {subfield_2.split(' ')[0]}",
            )
        for subfield_0 in marc_field.get_subfields("0"):
            code = ""
            if "(" in subfield_0 and ")" in subfield_0:
                code = subfield_0[subfield_0.find("(") + 1 : subfield_0.find(")")]
                code = code.split(" ")[0]
            elif url := urllib.parse.urlparse(subfield_0):
                if url.hostname:
                    code = subfield_0[: subfield_0.find(url.path)]
            if code:
                self.migration_report.add(
                    "AuthoritySources", i18n.t("$0 base uri or source code") + f": {code}"
                )

    def apply_rules(self, marc_field: pymarc.Field, mapping, legacy_ids):
        try:
            values = []
            value = ""
            if has_conditions(mapping):
                value = self.get_value_from_condition(",".join(legacy_ids), mapping, marc_field)
            elif has_value_to_add(mapping):
                value = mapping["rules"][0]["value"]
                if value == "false":
                    return [False]
                elif value == "true":
                    return [True]
                else:
                    return [value]
            elif not mapping.get("rules", []) or not mapping["rules"][0].get("conditions", []):
                values = self.handle_sub_field_delimiters(
                    ",".join(legacy_ids), mapping, marc_field
                )
                value = " ".join(values)
            values = wrap(value, 3) if mapping.get("subFieldSplit", "") else [value]
            return values
        except TransformationProcessError as trpe:
            self.handle_transformation_process_error(self.parsed_records, trpe)
        except TransformationFieldMappingError as fme:
            self.migration_report.add("FieldMappingErrors", fme.message)
            fme.data_value = (
                f"{fme.data_value} MARCField: {marc_field} Mapping: {json.dumps(mapping)}"
            )
            fme.log_it()
            return []
        except TransformationRecordFailedError as trfe:
            trfe.data_value = (
                f"{trfe.data_value} MARCField: {marc_field} Mapping: {json.dumps(mapping)}"
            )
            trfe.log_it()
            self.migration_report.add_general_statistics(
                i18n.t("Records failed due to an error. See data issues log for details")
            )
        except Exception as exception:
            self.handle_generic_exception(self.parsed_records, exception)

    def report_bad_tags(self, marc_field, bad_tags, legacy_ids):
        if (
            (not marc_field.tag.isnumeric())
            and marc_field.tag != "LDR"
            and marc_field.tag not in bad_tags
        ):
            self.migration_report.add("NonNumericTagsInRecord", marc_field.tag)
            message = "Non-numeric tags in records"
            Helper.log_data_issue(legacy_ids, message, marc_field.tag)
            bad_tags.add(marc_field.tag)

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
                if target not in rec and not schema_parent:  # have we added this already?
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
        if (
            self.task_configuration.migration_task_type == "BibsTransformer"
            and self.task_configuration.parse_cataloged_date
            and target_string == "catalogedDate"
        ):
            try:
                value = [str(parse(value[0], fuzzy=True).date())]
            except Exception as ee:
                Helper.log_data_issue("", f"Could not parse catalogedDate: {ee}", value)
                self.migration_report.add(
                    "FieldMappingErrors", i18n.t("Could not parse catalogedDate")
                )
        if not target_string or target_string not in sch:
            raise TransformationFieldMappingError(
                "",
                i18n.t("Target string '%{string}' not in Schema!", string=target_string)
                + i18n.t("Check mapping file against the schema.")
                + " "
                + i18n.t("Target type")
                + f": {sch.get(target_string,{}).get('type','')} "
                + i18n.t("Value")
                + f": {value}",
                "",
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
                (
                    f"Edge! Target string: {target_string} "
                    f"Target type: {sch.get(target_string,{}).get('type','')} Value: {value}"
                ),
            )

    def remove_from_id_map(self, former_ids: List[str]):
        """removes the ID from the map in case parsing failed

        Args:
            former_ids (_type_): _description_
        """
        for former_id in [id for id in former_ids if id]:
            if former_id in self.id_map:
                del self.id_map[former_id]

    def create_entity(
        self, entity_mappings, marc_field: Field, entity_parent_key, index_or_legacy_id
    ):
        entity = {}
        parent_schema_prop = self.schema.get("properties", {}).get(entity_parent_key, {})
        if parent_schema_prop.get("type", "") == "array":
            req_entity_props = parent_schema_prop.get("items", {}).get("required", [])
        elif parent_schema_prop.get("type", "") == "object":
            req_entity_props = parent_schema_prop.get("required", [])
        else:
            req_entity_props = []
        for entity_mapping in entity_mappings:
            k = entity_mapping["target"].split(".")[-1]
            if k == "authorityId" and (legacy_subfield_9 := marc_field.get("9")):
                marc_field.add_subfield("0", legacy_subfield_9)
                marc_field.delete_subfield("9")
            if my_values := [
                v
                for v in self.apply_rules(marc_field, entity_mapping, index_or_legacy_id)
                if v != ""
            ]:
                if entity_parent_key != k:
                    entity[k] = my_values[0]
                else:
                    entity = my_values[0]
            elif "alternativeMapping" in entity_mapping:
                alt_mapping = entity_mapping["alternativeMapping"]
                alt_k = alt_mapping["target"].split(".")[-1]
                if alt_values := [
                    v
                    for v in self.apply_rules(marc_field, alt_mapping, index_or_legacy_id)
                    if v != ""
                ]:
                    if entity_parent_key != alt_k:
                        entity[alt_k] = alt_values[0]
                    else:
                        entity = alt_values[0]
        missing_required_props = [
            req_entity_prop
            for req_entity_prop in req_entity_props
            if req_entity_prop not in entity
        ]
        return {} if any(missing_required_props) else entity

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
                entity = self.create_entity(entity_mapping, temp_field, e_parent, legacy_ids)
                if entity and (
                    (isinstance(entity, dict) and all(entity.values()))
                    or (isinstance(entity, list) and all(entity))
                ):
                    self.add_entity_to_record(entity, e_parent, folio_record, self.schema)
        else:
            if mapping.get("ignoreSubsequentSubfields", False):
                marc_field = self.remove_repeated_subfields(marc_field)
            entity = self.create_entity(entity_mapping, marc_field, e_parent, legacy_ids)
            if e_parent in ["precedingTitles", "succeedingTitles"]:
                self.create_preceding_succeeding_titles(entity, e_parent, folio_record["id"])
            elif entity and (
                all(
                    v
                    for k, v in entity.items()
                    if k not in ["staffOnly", "primary", "isbnValue", "issnValue"]
                )
                or e_parent in ["electronicAccess", "publication"]
                or (
                    e_parent.startswith("holdingsStatements") and any(v for k, v in entity.items())
                )
            ):
                self.add_entity_to_record(entity, e_parent, folio_record, self.schema)
            else:
                sfs = " - ".join(
                    f"{f[0]}:{('has_value' if f[1].strip() else 'empty')}" for f in marc_field
                )
                pattern = " - ".join(f"{k}:'{bool(v)}'" for k, v in entity.items())
                self.migration_report.add(
                    "IncompleteEntityMapping",
                    f"{marc_field.tag} {sfs} ->>-->> {e_parent} {pattern}  ",
                )
                # Experimental
                # self.add_entity_to_record(entity, e_parent, rec, self.schema)

    def handle_suppression(
        self, folio_record, file_def: FileDefinition, only_discovery_suppress: bool = False
    ):
        folio_record["discoverySuppress"] = file_def.discovery_suppressed
        self.migration_report.add(
            "Suppression",
            i18n.t("Suppressed from discovery") + f' = {folio_record["discoverySuppress"]}',
        )
        if not only_discovery_suppress:
            folio_record["staffSuppress"] = file_def.staff_suppressed
            self.migration_report.add(
                "Suppression", i18n.t("Staff suppressed") + f' = {folio_record["staffSuppress"]} '
            )

    def create_preceding_succeeding_titles(self, entity, e_parent, identifier):
        self.migration_report.add("PrecedingSuccedingTitles", f"{e_parent} " + i18n.t("created"))
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
        self.extradata_writer.write(e_parent, new_entity)

    def apply_rule(self, legacy_id, value, condition_types, marc_field, parameter):
        v = value
        for condition_type in iter(condition_types):
            try:
                v = self.conditions.get_condition(
                    condition_type, legacy_id, v, parameter, marc_field
                )
            except AttributeError as attr_error:
                raise TransformationProcessError(
                    legacy_id, attr_error, condition_type
                ) from attr_error
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
    def grouped(marc_field: Field):
        """Groups the subfields
        s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ...


        Args:
            marc_field (Field): _description_

        Returns:
            _type_: _description_
        """
        unique_subfields: list = []
        repeated_subfields: list = []
        results = []
        for sf, sf_vals in marc_field.subfields_as_dict().items():
            if len(sf_vals) == 1:
                unique_subfields.append(Subfield(code=sf, value=sf_vals[0]))
            else:
                repeated_subfields.extend([Subfield(code=sf, value=sf_val) for sf_val in sf_vals])
        if any(repeated_subfields):
            for repeated_subfield in repeated_subfields:
                new_subfields = [repeated_subfield]
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
        """Removes repeated subfields
        s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ...

        Args:
            marc_field (Field): _description_

        Returns:
            _type_: _description_
        """
        new_subfields = []
        for sf, sf_vals in marc_field.subfields_as_dict().items():
            new_subfields.extend([Subfield(code=sf, value=sf_vals[0])])
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
        legacy_ids: List[str],
        suppress: bool,
    ):
        """Saves the source Marc_record to the Source record Storage module

        Args:
            srs_records_file (_type_): _description_
            record_type (FOLIONamespaces): _description_
            folio_client (FolioClient): _description_
            marc_record (Record): _description_
            folio_record (_type_): _description_
            legacy_ids (List[str]): _description_
            suppress (bool): _description_
        """
        srs_id = RulesMapperBase.create_srs_id(record_type, folio_client.okapi_url, legacy_ids[-1])

        marc_record.add_ordered_field(
            Field(
                tag="999",
                indicators=["f", "f"],
                subfields=[
                    Subfield(code="i", value=folio_record["id"]),
                    Subfield(code="s", value=srs_id),
                ],
            )
        )
        # Since they all should be UTF encoded, make the leader align.
        try:
            temp_leader = Leader(marc_record.leader)
            temp_leader[9] = "a"
            marc_record.leader = temp_leader
        except Exception as ee:
            logging.exception(
                "Something is wrong with the marc records leader: %s, %s", marc_record.leader, ee
            )
        srs_record_string = RulesMapperBase.get_srs_string(
            marc_record,
            folio_record,
            srs_id,
            suppress,
            record_type,
        )
        srs_records_file.write(f"{srs_record_string}\n")

    @staticmethod
    def create_srs_id(record_type, okapi_url: str, legacy_id: str):
        srs_types = {
            FOLIONamespaces.holdings: FOLIONamespaces.srs_records_holdingsrecord,
            FOLIONamespaces.instances: FOLIONamespaces.srs_records_bib,
            FOLIONamespaces.authorities: FOLIONamespaces.srs_records_auth,
            FOLIONamespaces.edifact: FOLIONamespaces.srs_records_edifact,
        }

        return str(FolioUUID(okapi_url, srs_types.get(record_type), legacy_id))

    @staticmethod
    def get_bib_id_from_907y(marc_record: Record, index_or_legacy_id):
        try:
            return list(set(marc_record["907"].get_subfields("a", "y")))
        except Exception as e:
            raise TransformationRecordFailedError(
                index_or_legacy_id,
                (
                    "907 $y and $a is missing is missing, although they is "
                    "required for this legacy ILS choice"
                ),
                marc_record.as_json(),
            ) from e

    @staticmethod
    def get_bib_id_from_990a(marc_record: Record, index_or_legacy_id):
        res = {f["a"].strip() for f in marc_record.get_fields("990") if "a" in f}
        if marc_record["001"].format_field().strip():
            res.add(marc_record["001"].format_field().strip())
        if any(res):
            return list(res)
        else:
            raise TransformationRecordFailedError(
                index_or_legacy_id,
                "neither 990$a or 001 found in record.",
                marc_record.as_json(),
            )

    @staticmethod
    def get_bib_id_from_001(marc_record: Record, index_or_legacy_id):
        try:
            return [marc_record["001"].format_field().strip()]
        except Exception as e:
            raise TransformationRecordFailedError(
                index_or_legacy_id,
                "001 is missing, although it is required for Voyager migrations",
                marc_record.as_json(),
            ) from e

    @staticmethod
    def get_srs_string(
        marc_record: Record,
        folio_object: dict,
        srs_id,
        discovery_suppress: bool,
        record_type: FOLIONamespaces,
    ):
        record_types = {
            FOLIONamespaces.holdings: "MARC_HOLDING",
            FOLIONamespaces.instances: "MARC_BIB",
            FOLIONamespaces.authorities: "MARC_AUTHORITY",
            FOLIONamespaces.edifact: "EDIFACT",
        }

        id_holders = {
            FOLIONamespaces.instances: {
                "instanceId": folio_object["id"],
                "instanceHrid": folio_object.get("hrid", ""),
            },
            FOLIONamespaces.holdings: {
                "holdingsId": folio_object["id"],
                "holdingsHrid": folio_object.get("hrid", ""),
            },
            FOLIONamespaces.authorities: {
                "authorityId": folio_object["id"],
                "authorityHrid": marc_record["001"].data,
            },
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
            "additionalInfo": {"suppressDiscovery": discovery_suppress},
            "externalIdsHolder": id_holders.get(record_type),
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
