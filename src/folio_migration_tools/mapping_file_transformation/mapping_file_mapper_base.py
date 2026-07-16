"""Base class for all mapping file-based data transformations.

Provides the MappingFileMapperBase abstract class that all mapper classes inherit
from. Handles common functionality for loading mapping files, validating mappings,
and performing data transformations based on configured field mappings.
"""

import csv
import itertools
import json
import logging
import re
import uuid
from functools import reduce
from pathlib import Path
from typing import Any, Dict, List, Set
from uuid import UUID

import i18n
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

logger = logging.getLogger(__name__)


empty_vals = ["Not mapped", None, ""]


class MappingFileMapperBase(MapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        schema,
        record_map,
        statistical_codes_map,
        uuid_namespace: UUID,
        library_configuration: LibraryConfiguration,
        task_config: AbstractTaskConfiguration,
        ignore_legacy_identifier=False,
    ):
        """Initialize the base mapper for mapping file transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            schema: JSON schema for validation.
            record_map: Mapping configuration from legacy to FOLIO fields.
            statistical_codes_map: Mapping for statistical codes.
            uuid_namespace (UUID): UUID namespace for deterministic IDs.
            library_configuration (LibraryConfiguration): Library configuration.
            task_config (AbstractTaskConfiguration): Task configuration.
            ignore_legacy_identifier (bool): Whether to ignore legacy identifiers.
        """
        super().__init__(library_configuration, task_config, folio_client)
        self.uuid_namespace = uuid_namespace
        self.ignore_legacy_identifier = ignore_legacy_identifier
        self.schema = schema
        self.unique_record_ids: Set[str] = set()

        self.total_records = 0
        self.record_map = record_map
        self.ref_data_dicts: Dict = {}
        self.empty_vals = empty_vals
        self.folio_keys = self.get_mapped_folio_properties_from_map(self.record_map)
        self.field_map = self.setup_field_map(ignore_legacy_identifier)
        self.validate_map()
        try:
            self.mapped_from_values = {
                k["folio_field"]: k["value"]
                for k in self.record_map["data"]
                if k["value"] not in [None, ""] and k["folio_field"] != "legacyIdentifier"
            }
        except KeyError as ke:
            raise TransformationProcessError(
                "",
                "Property missing from one of the settings in the record mapping file",
                f"Property name: {ke}",
            ) from ke

        logger.info(
            "Mapped values:\n%s",
            json.dumps(self.mapped_from_values, indent=4, sort_keys=True),
        )
        legacy_fields = set()
        self.setup_statistical_codes_map(statistical_codes_map)
        self.legacy_record_mappings: dict = {}
        self.mapped_from_legacy_data: dict = {}
        for k in self.record_map["data"]:
            if (
                k["legacy_field"] not in self.empty_vals
                # or k["folio_field"] != "legacyIdentifier"
                or k["value"] not in self.empty_vals
            ):
                clean_folio_field = re.sub(r"\[\d+\]", "", k["folio_field"])
                self.legacy_record_mappings[k["folio_field"]] = list(
                    self.get_map_entries_by_folio_prop_name(
                        clean_folio_field, self.record_map["data"]
                    )
                )
                legacy_fields.add(k["legacy_field"])
                if not self.mapped_from_legacy_data.get(k["folio_field"]):
                    self.mapped_from_legacy_data[k["folio_field"]] = [k["legacy_field"]]
                elif k["legacy_field"] not in self.mapped_from_legacy_data[k["folio_field"]]:
                    self.mapped_from_legacy_data[k["folio_field"]].append(k["legacy_field"])

        logger.info(
            "Mapped legacy fields:\n%s",
            json.dumps(list(legacy_fields), indent=4, sort_keys=True),
        )
        logger.info(
            "Mapped FOLIO fields:\n%s",
            json.dumps(self.folio_keys, indent=4, sort_keys=True),
        )
        csv.register_dialect("tsv", delimiter="\t")

    def setup_field_map(self, ignore_legacy_identifier):
        field_map = {}  # Map of folio_fields and source fields as an array
        for k in self.record_map["data"]:
            if "folio_field" not in k:
                raise TransformationProcessError(
                    "", "Missing folio_field key in mapping", json.dumps(k)
                )
            if "legacy_field" not in k:
                raise TransformationProcessError(
                    "", "Missing legacy_field key in mapping", json.dumps(k)
                )

            if not field_map.get(k["folio_field"]):
                field_map[k["folio_field"]] = [k["legacy_field"]]
            else:
                field_map[k["folio_field"]].append(k["legacy_field"])
        if not ignore_legacy_identifier and "legacyIdentifier" not in field_map:
            raise TransformationProcessError(
                "",
                "property legacyIdentifier is not in map. Add this property "
                "to the mapping file as if it was a FOLIO property",
            )
        if not ignore_legacy_identifier:
            try:
                self.legacy_id_property_names = field_map["legacyIdentifier"]
                logger.info(
                    "Legacy identifier will be mapped from %s",
                    ",".join(self.legacy_id_property_names),
                )
            except Exception as exception:
                raise TransformationProcessError(
                    "",
                    f"property legacyIdentifier not setup in map: "
                    f"{field_map.get('legacyIdentifier', '')({exception})}",
                ) from exception
            del field_map["legacyIdentifier"]
        return field_map

    def validate_map(self):
        # TODO: Add functionality here to validate that the map is complete.
        # That it maps the required fields etc
        return True

    @staticmethod
    def get_mapped_folio_properties_from_map(the_map):
        return [
            k["folio_field"]
            for k in the_map["data"]
            if (
                k["legacy_field"] not in empty_vals
                # and k["folio_field"] != "legacyIdentifier"
                or k.get("value", "") not in empty_vals
                or isinstance(k.get("value", ""), bool)
            )
        ]

    @staticmethod
    def get_mapped_legacy_properties_from_map(the_map):
        return [
            k["legacy_field"].strip()
            for k in the_map["data"]
            if (k["legacy_field"].strip() not in empty_vals)
        ]

    def instantiate_record(
        self,
        legacy_object: dict,
        index_or_id,
        object_type: FOLIONamespaces,
        accept_duplicate_ids: bool = False,
    ):
        folio_object = {}
        if self.ignore_legacy_identifier:
            folio_object.update(
                {
                    "id": str(uuid.uuid4()),
                    "type": "object",
                }
            )
            return folio_object, index_or_id

        if not (
            legacy_id := " ".join(
                legacy_object.get(li, "") for li in self.legacy_id_property_names
            ).strip()
        ):
            raise TransformationRecordFailedError(
                index_or_id,
                "Could not get a value from legacy object from the property "
                f"{self.legacy_id_property_names}. Check mapping and data",
            )
        generated_id = str(
            FolioUUID(
                self.base_string_for_folio_uuid,
                object_type,
                legacy_id,
            )
        )
        if generated_id in self.unique_record_ids and not accept_duplicate_ids:
            raise TransformationRecordFailedError(
                index_or_id,
                "Legacy id already generated.",
                f"UUID: {generated_id}, seed: {legacy_id}",
            )
        else:
            self.unique_record_ids.add(generated_id)
        folio_object.update(
            {
                "id": generated_id,
                "type": "object",
            }
        )
        if object_type == FOLIONamespaces.holdings and hasattr(self, "holdings_sources"):
            folio_object["sourceId"] = self.holdings_sources.get("FOLIO")
        elif object_type == FOLIONamespaces.holdings and not hasattr(self, "holdings_sources"):
            raise TransformationProcessError(
                index_or_id, "Holdings source not set in the mapper", None
            )
        return folio_object, legacy_id

    def get_statistical_code(self, legacy_item: dict, folio_prop_name: str, index_or_id):
        if self.statistical_codes_mapping:
            return self.get_mapped_ref_data_value(
                self.statistical_codes_mapping,
                legacy_item,
                index_or_id,
                folio_prop_name,
                True,
            )
        self.migration_report.add(
            "StatisticalCodeMapping",
            i18n_t("Mapping not setup"),
        )
        return ""

    def get_prop(self, legacy_object, folio_prop_name, index_or_id, schema_default_value):
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        map_entries = list(
            MappingFileMapperBase.get_map_entries_by_folio_prop_name(
                folio_prop_name, self.record_map["data"]
            )
        )
        if not any(map_entries):
            return ""
        elif len(map_entries) > 1:
            self.migration_report.add(
                "Details", i18n.t("%{props} were concatenated", props=legacy_item_keys)
            )
            return " ".join(
                MappingFileMapperBase.get_legacy_value(
                    legacy_object,
                    map_entry,
                    self.migration_report,
                    index_or_id,
                    self.library_configuration.multi_field_delimiter,
                )
                for map_entry in map_entries
            ).strip()
        else:
            legacy_value = MappingFileMapperBase.get_legacy_value(
                legacy_object,
                map_entries[0],
                self.migration_report,
                index_or_id,
                self.library_configuration.multi_field_delimiter,
            )
            if legacy_value or isinstance(legacy_value, bool):
                return legacy_value
            else:
                self.migration_report.add(
                    "FolioDefaultValuesAdded",
                    i18n.t(
                        "%{schema_value} added to %{prop_name}",
                        schema_value=schema_default_value,
                        prop_name=folio_prop_name,
                    ),
                )
                return schema_default_value

    def do_map(
        self,
        legacy_object,
        index_or_id: str,
        object_type: FOLIONamespaces,
        accept_duplicate_ids=False,
    ) -> tuple[dict, str]:
        folio_object, legacy_id = self.instantiate_record(
            legacy_object, index_or_id, object_type, accept_duplicate_ids
        )
        for property_name, property in self.schema["properties"].items():
            try:
                self.map_property(property_name, property, folio_object, legacy_id, legacy_object)
            except TransformationFieldMappingError as data_error:
                self.handle_transformation_field_mapping_error(legacy_id, data_error)
        clean_folio_object = self.validate_required_properties(
            legacy_id, folio_object, self.schema, object_type
        )
        return (clean_folio_object, legacy_id)

    def map_property(
        self, schema_property_name: str, schema_property, folio_object, index_or_id, legacy_object
    ):
        if skip_property(schema_property_name, schema_property):
            pass
        elif schema_property.get("type", "") == "object":
            if "properties" in schema_property:
                self.map_object_props(
                    legacy_object,
                    schema_property_name,
                    schema_property,
                    folio_object,
                    index_or_id,
                    1,
                )
        elif schema_property.get("type", "") == "array":
            logger.debug("Mapping array property: %s.", schema_property_name)
            try:
                if schema_property["items"].get("type", "") == "object":
                    static_only_props = self.map_objects_array_props(
                        legacy_object,
                        schema_property_name,
                        schema_property["items"]["properties"],
                        folio_object,
                        index_or_id,
                        schema_property["items"].get("required", []),
                    )
                    self.validate_object_items_in_array(
                        folio_object,
                        schema_property_name,
                        schema_property,
                        index_or_id,
                        static_only_props,
                    )

                elif schema_property["items"].get("type", "") in ["string", "number", "integer"]:
                    self.map_string_array_props(
                        legacy_object,
                        schema_property_name,
                        folio_object,
                        index_or_id,
                    )
                else:
                    logger.info("Edge case %s", schema_property_name)

            except KeyError as schema_anomaly:
                logger.exception(
                    "Cannot create property '%s'. Unsupported schema format: %s",
                    schema_property_name,
                    schema_anomaly,
                )

        else:  # Basic property
            self.map_basic_props(
                legacy_object, schema_property_name, folio_object, index_or_id, schema_property
            )

    @staticmethod
    def get_legacy_value(
        legacy_object: dict,
        mapping_file_entry: dict,
        migration_report: MigrationReport,
        index_or_id: str = "",
        multi_field_delimiter="",
    ):
        # Mapping from value fields has preceedence and does not get involved in post processing
        if mapping_file_entry.get("value", "") or isinstance(
            mapping_file_entry.get("value", ""), bool
        ):
            value_mapped_value = mapping_file_entry.get("value")
            migration_report.add(
                "DefaultValuesAdded",
                i18n.t(
                    "%{value} added to %{entry}",
                    value=value_mapped_value,
                    entry=mapping_file_entry.get("folio_field", ""),
                ),
            )
            return value_mapped_value

        value, source_field = MappingFileMapperBase.resolve_mapped_legacy_value(
            legacy_object,
            mapping_file_entry,
            migration_report,
        )

        if not MappingFileMapperBase.should_apply_rules_for_source(
            source_field, mapping_file_entry
        ):
            return value

        return MappingFileMapperBase.apply_rules_to_resolved_value(
            value,
            source_field,
            mapping_file_entry,
            migration_report,
            multi_field_delimiter,
        )

    @staticmethod
    def should_apply_rules_for_source(source_field: str, mapping_file_entry: dict) -> bool:
        # fallback_value is a literal fallback and should bypass rules, like value.
        if source_field == "fallback_value":
            return False

        rules_apply_scope = mapping_file_entry.get("rules_apply_scope", "resolved_non_literal")
        if rules_apply_scope == "none":
            return False
        if rules_apply_scope == "legacy_only":
            return source_field == mapping_file_entry.get("legacy_field", "")

        return True

    @staticmethod
    def resolve_mapped_legacy_value(
        legacy_object: dict, mapping_file_entry: dict, migration_report
    ):
        primary_field = mapping_file_entry["legacy_field"]
        value = legacy_object.get(primary_field, "")
        if value:
            return value, primary_field

        fallback_field, fallback_value = MappingFileMapperBase.get_fallback_legacy_value(
            legacy_object,
            mapping_file_entry.get("fallback_legacy_field", ""),
        )
        if fallback_field and fallback_value not in ["", None]:
            migration_report.add(
                "FieldMappingDetails",
                (
                    f"Added fallback value from {fallback_field} "
                    f"instead of {mapping_file_entry['legacy_field']}"
                ),
            )
            return fallback_value, fallback_field

        fallback_value_literal = mapping_file_entry.get("fallback_value", "")
        if fallback_value_literal:
            migration_report.add(
                "FieldMappingDetails",
                (
                    f"Added fallback value {fallback_value_literal} "
                    f"instead of empty {mapping_file_entry['legacy_field']}"
                ),
            )
            return fallback_value_literal, "fallback_value"

        return value, primary_field

    @staticmethod
    def apply_rules_to_resolved_value(
        value,
        source_field,
        mapping_file_entry: dict,
        migration_report: MigrationReport,
        multi_field_delimiter="",
    ):
        if not value:
            return value

        if mapping_file_entry.get("rules", {}).get("regexGsub", ""):
            gsub_rule = mapping_file_entry.get("rules", {}).get("regexGsub", "")
            if not isinstance(gsub_rule, dict):
                raise ValueError("regexGsub must be a dictionary with 'pattern' and 'replacement'")

            pattern = gsub_rule.get("pattern", "")
            replacement = gsub_rule.get("replacement", "")
            value = re.sub(pattern, replacement, value)

        if mapping_file_entry.get("rules", {}).get("replaceValues", {}):
            if multi_field_delimiter and multi_field_delimiter in value:
                replaced_split_values = [
                    mapping_file_entry["rules"]["replaceValues"].get(sv, "")
                    for sv in value.split(multi_field_delimiter)
                ]
                replaced_val = multi_field_delimiter.join(replaced_split_values)
            else:
                replaced_val = mapping_file_entry["rules"]["replaceValues"].get(value, "")

            if replaced_val or isinstance(replaced_val, bool):
                migration_report.add(
                    "FieldMappingDetails",
                    f"Replaced {value} in {source_field} with {replaced_val}",
                )
                value = replaced_val

        if mapping_file_entry.get("rules", {}).get("regexGetFirstMatchOrEmpty", ""):
            my_pattern = (
                f"{mapping_file_entry.get('rules', {}).get('regexGetFirstMatchOrEmpty')}|$"
            )
            value = re.findall(my_pattern, value)[0]

        return value

    @staticmethod
    def normalize_fallback_legacy_fields(fallback_legacy_field):
        if isinstance(fallback_legacy_field, str):
            return (
                [fallback_legacy_field] if fallback_legacy_field.strip() not in empty_vals else []
            )
        if isinstance(fallback_legacy_field, list):
            return [
                field
                for field in fallback_legacy_field
                if isinstance(field, str) and field.strip() not in empty_vals
            ]
        return []

    @staticmethod
    def get_fallback_legacy_value(legacy_object: dict, fallback_legacy_field):
        fallback_fields = MappingFileMapperBase.normalize_fallback_legacy_fields(
            fallback_legacy_field
        )
        for fallback_field in fallback_fields:
            fallback_value = legacy_object.get(fallback_field, "")
            if isinstance(fallback_value, str):
                fallback_value = fallback_value.strip()
            if fallback_value not in ["", None]:
                return fallback_field, fallback_value
        return "", ""

    @staticmethod
    def get_legacy_vals(legacy_item, legacy_item_keys):
        result_list = []
        for legacy_item_key in legacy_item_keys:
            val = legacy_item.get(legacy_item_key, "")
            if val not in ["", None]:
                result_list.append(val)
        return result_list

    def map_object_props(
        self,
        legacy_object,
        schema_property_name: str,
        schema_property,
        folio_object,
        index_or_id,
        level: int,
    ):
        def _normalize_local_object_path(path: str) -> str:
            # When mapping nested object fields under an array item (for example,
            # "poLines[0].cost.currency") into a temporary item object, keep only
            # the local path segment ("cost.currency").
            return path.split("].", 1)[1] if "]." in path else path

        temp_object: dict = {}
        for child_property_name, child_property in schema_property["properties"].items():
            sub_prop_path = f"{schema_property_name}.{child_property_name}"
            if "properties" in child_property:
                self.map_object_props(
                    legacy_object,
                    sub_prop_path,
                    child_property,
                    folio_object,
                    index_or_id,
                    level + 1,
                )
            elif (
                child_property.get("type", "") == "array"
                and child_property.get("items", {}).get("type", "") == "object"
                and child_property.get("items", {}).get("properties", "")
            ):
                static_only_props = self.map_objects_array_props(
                    legacy_object,
                    f"{schema_property_name}.{child_property_name}",
                    child_property["items"]["properties"],
                    folio_object,
                    index_or_id,
                    [],
                )
                self.validate_object_items_in_array(
                    folio_object,
                    f"{schema_property_name}.{child_property_name}",
                    child_property,
                    index_or_id,
                    static_only_props,
                )

            elif child_property.get("type", "") == "array" and child_property.get("items", {}).get(
                "type", ""
            ) in ["string", "number", "integer"]:
                self.map_string_array_props(
                    legacy_object,
                    f"{schema_property_name}.{child_property_name}",
                    folio_object,
                    index_or_id,
                )
            elif child_property.get("type", "") in ["string", "number", "integer"]:
                if p := self.get_prop(
                    legacy_object, sub_prop_path, index_or_id, child_property.get("default", "")
                ):
                    set_at_path(folio_object, _normalize_local_object_path(sub_prop_path), p)
                # temp_object[child_property_name] = p
            elif p := self.get_prop(
                legacy_object, sub_prop_path, index_or_id, child_property.get("default", "")
            ):
                set_at_path(folio_object, _normalize_local_object_path(sub_prop_path), p)
        if temp_object:
            set_deep(folio_object, schema_property_name, temp_object)
            # folio_object[schema_property_name] = temp_object

    def map_objects_array_props(  # noqa: C901
        self,
        legacy_object,
        prop_name: str,
        sub_properties,
        folio_object: dict,
        index_or_id,
        required: list[str],
    ):
        resulting_array = []
        static_props = set()
        legacy_props = set()
        for entry in self.record_map["data"]:
            m = re.match(rf"{re.escape(prop_name)}\[\d+\]\.(.+)", entry["folio_field"])
            if m:
                sub_prop = m.group(1)
                if entry.get("value") not in [None, ""] or isinstance(entry.get("value"), bool):
                    static_props.add(sub_prop)
                if entry["legacy_field"] not in empty_vals:
                    legacy_props.add(sub_prop)
        static_only_props = static_props - legacy_props
        i = 0
        while True:
            keys_to_map = {
                k.rsplit(".", 1)[0] for k in self.folio_keys if k.startswith(f"{prop_name}[{i}")
            }
            if not any(keys_to_map):
                break
            for _ in keys_to_map:
                temp_object = {}
                multi_field_props: List[str] = []
                for sub_prop_name, sub_prop in (
                    (k, p)
                    for k, p in sub_properties.items()
                    if not p.get("folio:isVirtual", False)
                ):
                    prop_path = f"{prop_name}[{i}].{sub_prop_name}"
                    if prop_path in self.folio_keys:
                        # We have reached the end of the prop path?
                        res = self.get_prop(
                            legacy_object,
                            prop_path,
                            index_or_id,
                            sub_properties[sub_prop_name].get("default", ""),
                        )
                        self.report_legacy_mapping(
                            self.legacy_basic_property(prop_path), True, True
                        )

                        if (
                            isinstance(res, str)
                            and self.library_configuration.multi_field_delimiter in res
                        ):
                            for delim_value in res.split(
                                self.library_configuration.multi_field_delimiter
                            ):
                                if delim_value not in empty_vals:
                                    self.validate_enums(
                                        delim_value,
                                        sub_prop,
                                        sub_prop_name,
                                        index_or_id,
                                        required,
                                    )
                            multi_field_props.append(sub_prop_name)
                        else:
                            self.validate_enums(
                                res, sub_prop, sub_prop_name, index_or_id, required
                            )

                        if res or isinstance(res, bool):
                            temp_object[sub_prop_name] = res

                    elif (
                        sub_prop_name in sub_properties
                        and sub_properties[sub_prop_name].get("type", "") == "array"
                        and sub_properties[sub_prop_name]["items"].get("type", "") == "object"
                    ):
                        self.map_objects_array_props(
                            legacy_object,
                            prop_path,
                            sub_properties[sub_prop_name]["items"]["properties"],
                            folio_object,
                            index_or_id,
                            [],
                        )
                    elif (
                        sub_prop_name in sub_properties
                        and sub_properties[sub_prop_name].get("type", "") == "array"
                        and sub_properties[sub_prop_name]["items"].get("type", "")
                        in ["string", "number", "integer"]
                    ):
                        # We have not reached the end of the prop path
                        for array_path in [p for p in self.folio_keys if p.startswith(prop_path)]:
                            res = self.get_prop(
                                legacy_object,
                                array_path,
                                index_or_id,
                                sub_properties[sub_prop_name].get("default", ""),
                            )
                            self.validate_enums(
                                res, sub_prop, sub_prop_name, index_or_id, required
                            )
                            if res or isinstance(res, bool):
                                self.add_values_to_string_array(
                                    sub_prop_name,
                                    temp_object,
                                    res,
                                    self.library_configuration.multi_field_delimiter,
                                )

                    elif sub_prop.get("type", "") == "object" and "properties" in sub_prop:
                        self.map_object_props(
                            legacy_object, prop_path, sub_prop, temp_object, index_or_id, 0
                        )
            i = i + 1

            if any(multi_field_props):
                resulting_array.extend(
                    self.split_obj_by_delim(
                        self.library_configuration.multi_field_delimiter,
                        temp_object,
                        multi_field_props,
                    )
                )
            else:
                resulting_array.append(temp_object)

        if any(resulting_array):
            set_deep2(folio_object, prop_name, resulting_array)

        return static_only_props

    @staticmethod
    def split_obj_by_delim(delimiter: str, folio_obj: dict, delimited_props: List[str]):
        non_split_props = [(k, v) for k, v in folio_obj.items() if k not in delimited_props]
        delimited_props_split = [[x, *folio_obj[x].split(delimiter)] for x in delimited_props]
        zipped = list(zip(*delimited_props_split, strict=False))
        res = []
        for (prop_name_idx, prop_name), (value_idx, ra) in itertools.product(
            enumerate(zipped[0]), enumerate(zipped[1:])
        ):
            if prop_name_idx == 0:
                res.append({prop_name: ra[prop_name_idx]})
            else:
                res[value_idx][prop_name] = ra[prop_name_idx]
        for r in res:
            r.update(non_split_props)
        return res

    def map_string_array_props(self, legacy_object, prop, folio_object, index_or_id):
        keys_to_map = [k for k in self.folio_keys if k.startswith(prop)]
        for prop_name in keys_to_map:
            if prop_name in self.folio_keys and self.has_property(legacy_object, prop_name):
                if mapped_prop := self.get_prop(legacy_object, prop_name, index_or_id, ""):
                    self.add_values_to_string_array(
                        prop,
                        folio_object,
                        mapped_prop,
                        self.library_configuration.multi_field_delimiter,
                    )
                self.report_legacy_mapping(self.legacy_basic_property(prop_name), True, True)

    @staticmethod
    def add_values_to_string_array(prop, folio_object, mapped_prop_value, delimiter: str):
        if in_deep(folio_object, prop) and mapped_prop_value not in get_deep(
            folio_object, prop, []
        ):
            if isinstance(mapped_prop_value, str) and delimiter in mapped_prop_value:
                old_prop = get_deep(folio_object, prop)
                set_deep(folio_object, prop, old_prop.extend(mapped_prop_value.split(delimiter)))

            else:
                old_prop = get_deep(folio_object, prop)
                added_prop = old_prop.append(mapped_prop_value)
                set_deep(folio_object, prop, [added_prop])
        elif isinstance(mapped_prop_value, str) and delimiter in mapped_prop_value:
            set_deep(folio_object, prop, mapped_prop_value.split(delimiter))
        else:
            # No values in array previously
            set_deep(folio_object, prop, [mapped_prop_value])

    def map_basic_props(
        self, legacy_object, property_name, folio_object, index_or_id, schema_property
    ):
        if self.has_basic_property(legacy_object, property_name):  # is there a match in the csv?
            mapped_prop = self.get_prop(
                legacy_object, property_name, index_or_id, schema_property.get("default", "")
            )
            if mapped_prop or isinstance(mapped_prop, bool):
                self.validate_enums(
                    mapped_prop,
                    schema_property,
                    property_name,
                    index_or_id,
                    self.schema.get("required", []),
                )
                folio_object[property_name] = mapped_prop
            self.report_legacy_mapping(self.legacy_basic_property(property_name), True, True)

    @staticmethod
    def _get_delimited_file_reader(source_file, file_name: Path):
        """Count rows in source file and return counts with a DictReader.

        Counts total rows and empty rows, then returns those counts
        along with a csv.DictReader.

        Args:
            source_file (_type_): _description_
            file_name (Path): _description_

        Returns:
            (int, int, DictReader): total rows, empty rows, dict reader
        """
        empty_rows = 0
        total_rows = -1  # Do not count header row
        if str(file_name).endswith("tsv"):
            delimiter = "\t"
        else:
            delimiter = ","
        for line in source_file:
            if not "".join(line.strip().split(delimiter)):  # check for empty rows
                empty_rows += 1
            total_rows += 1
        source_file.seek(0)  # Set file position back to start
        if str(file_name).endswith("tsv"):
            dict_reader = csv.DictReader(source_file, dialect="tsv")
        else:
            dict_reader = csv.DictReader(source_file)
        return total_rows, empty_rows, dict_reader

    def get_objects(self, source_file, file_name: Path):
        total_rows, empty_rows, reader = self._get_delimited_file_reader(source_file, file_name)
        logger.info("Source data file contains %d rows", total_rows)
        logger.info("Source data file contains %d empty rows", empty_rows)
        self.migration_report.set(
            "GeneralStatistics", "Number of rows in {}".format(file_name.name), total_rows
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Number of empty rows in {}".format(file_name.name),
            empty_rows,
        )
        try:
            yield from reader
        except Exception as exception:
            logger.exception("%s at row %s", exception, reader.line_num)
            raise exception from exception

    def has_property(self, legacy_object, folio_prop_name: str):
        legacy_keys = self.field_map.get(folio_prop_name, [])
        return (
            any(legacy_keys)
            and any(k not in empty_vals for k in legacy_keys)
            and any(legacy_object.get(legacy_key, "") for legacy_key in legacy_keys)
        )

    def has_basic_property(self, legacy_object, folio_prop_name):
        if folio_prop_name not in self.folio_keys:
            return False
        if folio_prop_name in self.mapped_from_values:
            return True
        legacy_mappings = self.legacy_record_mappings.get(folio_prop_name, [])

        return any(legacy_mappings) and any(
            legacy_mapping not in empty_vals for legacy_mapping in legacy_mappings
        )

    @staticmethod
    def get_map_entries_by_folio_prop_name(folio_prop_name, data):
        return (
            k
            for k in data
            if k["folio_field"] == folio_prop_name
            and any(
                is_set_or_bool_or_numeric(k.get(key, ""))
                for key in ("value", "legacy_field", "fallback_legacy_field", "fallback_value")
            )
        )

    def legacy_basic_property(self, folio_prop):
        if folio_prop not in self.folio_keys:
            return ""
        return next(
            (k["legacy_field"] for k in self.record_map["data"] if k["folio_field"] == folio_prop),
            "",
        )

    def verify_legacy_record(self, legacy_object, idx):
        if idx == 0:
            missing_keys_in_record = [
                f
                for f in self.get_mapped_legacy_properties_from_map(self.record_map)
                if f not in legacy_object
            ]
            if any(missing_keys_in_record):
                raise TransformationProcessError(
                    "",
                    ("There are mapped legacy fields that are not in the legacy record"),
                    missing_keys_in_record,
                )
            else:
                logger.info("All mapped legacy fields are in the legacy object")

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        if ref_object := self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ()):
            return ref_object
        d = {r[key_type].lower(): (r["id"], r["name"]) for r in ref_data}
        self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ())

    def validate_enums(
        self,
        mapped_value,
        mapped_schema_property,
        mapped_schema_property_name,
        index_or_id,
        required,
    ):
        if (
            (
                "enum" in mapped_schema_property
                and mapped_value
                and mapped_value not in mapped_schema_property["enum"]
            )
            or (
                "enum" in mapped_schema_property
                and mapped_schema_property_name in required
                and not mapped_value
            )
            or (
                mapped_schema_property.get("items", {}).get("enum")
                and mapped_value
                and mapped_value not in mapped_schema_property.get("items", {}).get("enum")
            )
        ):
            raise TransformationRecordFailedError(
                index_or_id,
                f"Allowed values for {mapped_schema_property_name} "
                f"are {mapped_schema_property['enum']} "
                f"Forbidden enum value found: ",
                mapped_value,
            )

    def is_uuid(self, value):
        """Returns True if the value is a UUID, and False if it is not.

        Args:
            value (_type_): a value that may or may not be a UUID

        Returns:
            bool: True/False
        """
        try:
            uuid.UUID(str(value))
        except ValueError:
            return False
        return True

    def validate_object_items_in_array(
        self,
        folio_object,
        property_path,
        schema_property,
        index_or_id="",
        static_only_props: set | None = None,
    ):
        valid_array_objects = []
        static_only_props = static_only_props or set()
        required = schema_property.get("items", {}).get("required", [])
        array_items = get_from_path(folio_object, property_path, [])
        logger.debug(
            "Validating object items in array for property: %s. Objects being validated: %s",
            property_path,
            json.dumps(array_items, indent=2),
        )
        for item in array_items:
            try:
                if not isinstance(item, dict):
                    valid_array_objects.append(item)
                    continue
                if all(item.get(r) or (isinstance(item.get(r), bool)) for r in required):
                    valid_array_objects.append(item)
                elif any(
                    (v or isinstance(v, bool))
                    for k, v in item.items()
                    if k not in static_only_props
                ):
                    # Item has real content but is missing required fields: log as data issue
                    # and discard
                    missing = [
                        r for r in required if not (item.get(r) or isinstance(item.get(r), bool))
                    ]
                    raise TransformationFieldMappingError(
                        index_or_id,
                        f"Required properties missing in {property_path} item",
                        json.dumps(missing),
                    )
                else:
                    # Truly empty item — silently discard
                    self.migration_report.add(
                        "IncompleteSubPropertyRemoved",
                        f"{property_path}",
                    )
            except TransformationFieldMappingError as data_error:
                self.handle_transformation_field_mapping_error(index_or_id, data_error)
        if valid_array_objects:
            set_at_path(folio_object, property_path, valid_array_objects)
        else:
            pop_at_path(folio_object, property_path)

    @staticmethod
    def has_call_number_parts(folio_rec: Dict[str, Any]) -> bool:
        """Check if a FOLIO record has any call number parts (excluding call number type ID).

        Args:
            folio_rec (Dict[str, Any]): A FOLIO record represented as a dictionary
        Returns:
            bool: True if any call number parts exist, False otherwise
        """
        return any(
            folio_rec.get(part)
            for part in folio_rec.keys()
            if (
                "callnumbertypeid" not in part.lower()
                and ("callnumber" in part.lower() and "additional" not in part.lower())
            )
        )

    @staticmethod
    def get_call_number_type_id_by_name(
        folio_client: FolioClient, call_number_type_name: str
    ) -> str:
        """Look up call number type ID by name from FOLIO.

        Args:
            folio_client (FolioClient): FOLIO API client.
            call_number_type_name (str): The name of the call number type to look up.

        Returns:
            str: The UUID of the matching call number type.

        Raises:
            TransformationProcessError: If the call number type is not found in the tenant.
        """
        try:
            return {x["name"]: x["id"] for x in folio_client.call_number_types}[
                call_number_type_name
            ]
        except KeyError as ke:
            raise TransformationProcessError(
                "",
                f"Default call number type '{call_number_type_name}' not found in tenant.",
            ) from ke


def skip_property(property_name: str, property: Dict[str, Any]) -> bool:
    return bool(
        property_name in ["metadata", "id", "lastCheckIn"]
        or property_name.startswith("effective")
        or property.get("folio:isVirtual", False)
        or property.get("description", "") == "Deprecated"
    )


def weird_division(number, divisor):
    return number / divisor if divisor else 0


def set_deep(dictionary, key, value):
    """Sets a nested property in a dict given a dot notated address.

    Args:
        dictionary (_type_): a python dictionary ({"a":{"b":{"c":"value"}}})
        key (_type_): A string of dot notated address (a.b.c)
        value (_type_): the value to set

    """
    dd = dictionary
    keys = key.split(".")
    latest = keys.pop()
    for k in keys:
        dd = dd.setdefault(k, {})
    dd.setdefault(latest, value)


def _split_path_token(path_part: str) -> tuple[str, int | None]:
    """Parse a path token into a dictionary key and optional list index.

    Args:
        path_part (str): A single path token such as "addresses" or
            "addresses[0]".

    Returns:
        tuple[str, int | None]: A tuple containing the dictionary key and an
            optional list index. If the token does not match the expected
            pattern, returns ``(path_part, None)``.
    """
    match = re.match(r"^([^\[\]]+)(?:\[(\d+)\])?$", path_part)
    if not match:
        return path_part, None
    key = match.group(1)
    idx = int(match.group(2)) if match.group(2) is not None else None
    return key, idx


def get_from_path(dictionary, path: str, default=None):
    """Read a value from a nested mapping/list structure by path.

    Supports dot notation for dictionaries (for example, ``a.b``) and bracket
    notation for list indexes (for example, ``a[0].b``).

    Args:
        dictionary (Any): Root object to read from.
        path (str): Dot/bracket path expression.
        default (Any, optional): Value returned when the path is missing or
            type-incompatible.

    Returns:
        Any: The value at the path if present; otherwise ``default``.
    """
    current = dictionary
    for part in path.split("."):
        key, idx = _split_path_token(part)
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current.get(key)
        if idx is not None:
            if not isinstance(current, list) or idx >= len(current):
                return default
            current = current[idx]
    return current


def set_at_path(dictionary, path: str, value):
    """Set a value at a nested path, creating intermediate containers as needed.

    Supports dot notation for dictionaries and bracket notation for list
    indexes. Missing intermediate nodes are created as dictionaries or lists.
    If an intermediate node has an incompatible type, the function exits
    without raising.

    Args:
        dictionary (Any): Root object to mutate.
        path (str): Dot/bracket path expression.
        value (Any): Value to set at the resolved path.
    """
    current = dictionary
    parts = path.split(".")
    for part in parts[:-1]:
        key, idx = _split_path_token(part)
        if not isinstance(current, dict):
            return
        if key not in current or current[key] is None:
            current[key] = [] if idx is not None else {}
        if idx is None:
            if not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        else:
            if not isinstance(current[key], list):
                current[key] = []
            while len(current[key]) <= idx:
                current[key].append({})
            if not isinstance(current[key][idx], dict):
                current[key][idx] = {}
            current = current[key][idx]

    key, idx = _split_path_token(parts[-1])
    if not isinstance(current, dict):
        return
    if idx is None:
        current[key] = value
    else:
        if key not in current or not isinstance(current[key], list):
            current[key] = []
        while len(current[key]) <= idx:
            current[key].append(None)
        current[key][idx] = value


def pop_at_path(dictionary, path: str):
    """Remove a value at a nested path if present.

    This helper is tolerant of missing paths and type mismatches. It performs
    no operation in those cases and does not raise.

    Args:
        dictionary (Any): Root object to mutate.
        path (str): Dot/bracket path expression to remove.
    """
    current = dictionary
    parts = path.split(".")
    for part in parts[:-1]:
        key, idx = _split_path_token(part)
        if not isinstance(current, dict) or key not in current:
            return
        current = current[key]
        if idx is not None:
            if not isinstance(current, list) or idx >= len(current):
                return
            current = current[idx]

    key, idx = _split_path_token(parts[-1])
    if not isinstance(current, dict) or key not in current:
        return
    if idx is None:
        current.pop(key, None)
    elif isinstance(current[key], list) and idx < len(current[key]):
        del current[key][idx]


def set_deep2(dictionary, key, value):
    """Sets a nested property in a dict given a dot notated address.

    Args:
        dictionary (_type_): a python dictionary ({"a":{"b":{"c":"value"}}})
        key (_type_): A string of dot notated address (a.b.c)
        value (_type_): the value to set

    """
    dd = dictionary
    keys = key.split(".")
    latest = keys.pop()
    name = ""
    number = 0
    for k in keys:
        if k == keys[0] and k.endswith("]"):
            m = re.search(r"\[([\d]+)\]", k)
            if m:
                number = int(m[1])
            else:
                raise ValueError(f"No list index found in key: {k}")
            name = k.split("[")[0]
            dd = dd.setdefault(name, [{}])
        else:
            dd = dd.setdefault(k, {})
    if name and keys and keys[0].startswith(name):
        if len(dd) <= number:
            dd.append({})
        dd[number][latest] = value
    elif latest in dd:
        for i in range(len(value)):
            if len(dd[latest]) > i and dd[latest][i] and isinstance(dd[latest][i], dict):
                dd[latest][i].update(value[i])
            else:
                dd[latest].insert(i, value[i])

    else:
        dd[latest] = value


def get_deep(dictionary, keys, default=None):
    """Returns a nested property in a dict given a dot notated address.

    Args:
        dictionary (_type_): a python dictionary ({"a":{"b":{"c":"value"}}})
        keys (_type_): A string of dot notated address (a.b.c)
        default (_type_): Default value to return

    Returns:
        _type_: the value/property of the dict
    """
    return reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else default,
        keys.split("."),
        dictionary,
    )


def in_deep(dictionary, keys):
    """Checks if a property exists given a dot notated address.

    Args:
        dictionary (_type_): a python dictionary ({"a":{"b":{"c":"value"}}})
        keys (_type_): A string of dot notated address (a.b.c)

    Returns:
        _type_: a truthy value or False is there is a property in the dict
    """
    return reduce(
        lambda d, key: d.get(key, False) if isinstance(d, dict) else False,
        keys.split("."),
        dictionary,
    )


def is_set_or_bool_or_numeric(any_value):
    if isinstance(any_value, list):
        return any(is_set_or_bool_or_numeric(value) for value in any_value)
    return (isinstance(any_value, str) and (any_value.strip() not in empty_vals)) or isinstance(
        any_value, (int, float, complex)
    )
