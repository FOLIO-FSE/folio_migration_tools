import csv
import json
import logging
from abc import abstractmethod
from pathlib import Path
from uuid import UUID
import uuid

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from folioclient import FolioClient
from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs

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
        ignore_legacy_identifier=False,
    ):
        super().__init__(library_configuration, folio_client)
        self.uuid_namespace = uuid_namespace
        self.ignore_legacy_identifier = ignore_legacy_identifier
        self.schema = schema
        self.total_records = 0
        self.folio_client = folio_client
        self.use_map = True  # Legacy
        self.record_map = record_map
        self.record_map = record_map
        self.ref_data_dicts = {}
        self.empty_vals = empty_vals
        self.folio_keys = self.get_mapped_folio_properties_from_map(self.record_map)
        self.field_map = self.setup_field_map(ignore_legacy_identifier)
        self.validate_map()
        self.mapped_from_values = {}
        for k in self.record_map["data"]:
            if k["value"] not in [None, ""] and k["folio_field"] != "legacyIdentifier":
                self.mapped_from_values[k["folio_field"]] = k["value"]
        logging.info(
            "Mapped values:\n%s",
            json.dumps(self.mapped_from_values, indent=4, sort_keys=True),
        )
        legacy_fields = set()
        if statistical_codes_map:
            self.statistical_codes_mapping = RefDataMapping(
                self.folio_client,
                "/statistical-codes",
                "statisticalCodes",
                statistical_codes_map,
                "code",
                Blurbs.StatisticalCodeMapping,
            )
            logging.info("Statistical codes mapping set up")
        else:
            self.statistical_codes_mapping = None
            logging.info("Statistical codes map is not set up")
        self.mapped_from_legacy_data = {}
        for k in self.record_map["data"]:
            if (
                k["legacy_field"] not in self.empty_vals
                # or k["folio_field"] != "legacyIdentifier"
                or k["value"] not in self.empty_vals
            ):
                legacy_fields.add(k["legacy_field"])
                if not self.mapped_from_legacy_data.get(k["folio_field"]):
                    self.mapped_from_legacy_data[k["folio_field"]] = {k["legacy_field"]}
                else:
                    self.mapped_from_legacy_data[k["folio_field"]].add(
                        k["legacy_field"]
                    )

        logging.info(
            "Mapped legacy fields:\n%s",
            json.dumps(list(legacy_fields), indent=4, sort_keys=True),
        )
        logging.info(
            "Mapped FOLIO fields:\n%s",
            json.dumps(self.folio_keys, indent=4, sort_keys=True),
        )
        csv.register_dialect("tsv", delimiter="\t")

    def setup_field_map(self, ignore_legacy_identifier):
        field_map = {}  # Map of folio_fields and source fields as an array
        for k in self.record_map["data"]:
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
                self.legacy_id_property_name = field_map["legacyIdentifier"][0]
                logging.info(
                    "Legacy identifier will be mapped from %s",
                    self.legacy_id_property_name,
                )
            except Exception as exception:
                raise TransformationProcessError(
                    "",
                    f"property legacyIdentifier not setup in map: "
                    f"{field_map.get('legacyIdentifier', '') ({exception})}",
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
            )
        ]

    def instantiate_record(
        self, legacy_object: dict, index_or_id, object_type: FOLIONamespaces
    ):

        if self.ignore_legacy_identifier:
            return ({}, str(uuid.uuid4()))

        legacy_id = legacy_object.get(self.legacy_id_property_name)
        if not legacy_id:
            raise TransformationRecordFailedError(
                index_or_id,
                "Could not get a value from legacy object from the property "
                f"{self.legacy_id_property_name}. Check mapping and data",
            )
        return (
            {
                "id": str(
                    FolioUUID(
                        self.folio_client.okapi_url,
                        object_type,
                        legacy_id,
                    )
                ),
                "metadata": self.folio_client.get_metadata_construct(),
                "type": "object",
            },
            legacy_id,
        )

    def get_statistical_codes(
        self, legacy_item: dict, folio_prop_name: str, index_or_id
    ):
        if self.statistical_codes_mapping:
            return self.get_mapped_value(
                self.statistical_codes_mapping,
                legacy_item,
                index_or_id,
                folio_prop_name,
                True,
            )
        self.migration_report.add(
            Blurbs.StatisticalCodeMapping,
            "Mapping not setup",
        )
        return ""

    @abstractmethod
    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        raise NotImplementedError(
            "This method needs to be implemented in a implementing class"
        )

    def do_map(
        self, legacy_object, index_or_id: str, object_type: FOLIONamespaces
    ) -> tuple[dict, str]:
        folio_object, legacy_id = self.instantiate_record(
            legacy_object, index_or_id, object_type
        )
        for property_name_level1, property_level1 in self.schema["properties"].items():
            try:
                self.map_level1_property(
                    property_name_level1,
                    property_level1,
                    folio_object,
                    legacy_id,
                    legacy_object,
                )
            except TransformationFieldMappingError as data_error:
                self.handle_transformation_field_mapping_error(legacy_id, data_error)
        clean_folio_object = self.validate_required_properties(
            legacy_id, folio_object, self.schema, object_type
        )
        return (clean_folio_object, legacy_id)

    def map_level1_property(
        self,
        property_name_level1,
        property_level1,
        folio_object,
        index_or_id,
        legacy_object,
    ):
        if property_level1.get("description", "") == "Deprecated" or skip_property(
            property_name_level1, property_level1
        ):
            pass
        elif property_level1["type"] == "object":
            if "properties" in property_level1:
                self.map_object_props(
                    legacy_object,
                    property_name_level1,
                    property_level1,
                    folio_object,
                    index_or_id,
                )
        elif property_level1["type"] == "array":
            if property_level1["items"]["type"] == "object":
                self.map_objects_array_props(
                    legacy_object,
                    property_name_level1,
                    property_level1["items"]["properties"],
                    folio_object,
                    index_or_id,
                    property_level1["items"].get("required", []),
                )
            elif property_level1["items"]["type"] == "string":
                self.map_string_array_props(
                    legacy_object,
                    property_name_level1,
                    folio_object,
                    index_or_id,
                )
            else:
                logging.info("Edge case %s", property_name_level1)
        else:  # Basic property
            self.map_basic_props(
                legacy_object, property_name_level1, folio_object, index_or_id
            )

    @staticmethod
    def get_legacy_vals(legacy_item, legacy_item_keys):
        return {
            legacy_item[k]
            for k in legacy_item_keys
            if legacy_item.get(k, "") not in ["", None]
        }

    def map_object_props(
        self,
        legacy_object,
        property_name_level1,
        property_level1,
        folio_object,
        index_or_id,
    ):
        temp_object = {}
        prop_key = property_name_level1
        for property_name_level2, property_level2 in property_level1[
            "properties"
        ].items():
            sub_prop_key = f"{prop_key}.{property_name_level2}"
            if "properties" in property_level2:
                for property_name_level3, property_level3 in property_level2[
                    "properties"
                ].items():
                    # not parsing stuff on level three.
                    pass
            elif property_level2["type"] == "array":
                """
                # Object with subprop array
                temp_object[property_name_level2] = []
                for i in range(5):
                    prop_path = f"{sub_prop_key}.{sub_prop_name2}[{i}]"
                    if property_level2["items"]["type"] == "object":
                        # Array of objects
                        temp = {
                            sub_prop_name2: self.get_prop(
                                folio_object,
                                ,
                                index_or_id,
                            )
                            for sub_prop_name2, sub_prop2 in property_level2["items"][
                                "properties"
                            ].items()
                        }
                        if not all(value for key, value in temp.items()):
                            continue
                        temp_object[property_name_level2].append(temp)
                    else:

                        mkey = sub_prop_key + "." + sub_prop_name2
                        a = self.get_prop(legacy_object, mkey, index_or_id, i)
                        if a:
                            temp_object[property_name_level2] = a"""
            elif p := self.get_prop(legacy_object, sub_prop_key, index_or_id):
                temp_object[property_name_level2] = p
        if temp_object:
            folio_object[property_name_level1] = temp_object

    def map_objects_array_props(
        self,
        legacy_object,
        prop_name: str,
        sub_properties,
        folio_object: dict,
        index_or_id,
        required: list[str],
    ):
        resulting_array = []
        keys_to_map = {
            k.split(".")[0] for k in self.folio_keys if k.startswith(prop_name)
        }
        for object_key in keys_to_map:
            temp_object = {}
            for prop in (
                k
                for k, p in sub_properties.items()
                if not p.get("folio:isVirtual", False)
            ):
                prop_path = f"{object_key}.{prop}"
                if prop_path in self.folio_keys:
                    res = self.get_prop(legacy_object, prop_path, index_or_id)
                    self.report_legacy_mapping(
                        self.legacy_basic_property(prop), True, True
                    )
                    temp_object[prop] = res

            if temp_object != {} and all(
                (
                    v or (isinstance(v, bool))
                    for k, v in temp_object.items()
                    if k in required
                )
            ):
                resulting_array.append(temp_object)
        if any(resulting_array):
            folio_object[prop_name] = resulting_array

    def map_string_array_props(self, legacy_object, prop, folio_object, index_or_id):
        keys_to_map = [k for k in self.folio_keys if k.startswith(prop)]
        for prop_name in keys_to_map:
            if prop_name in self.folio_keys and self.has_property(
                legacy_object, prop_name
            ):
                if mapped_prop := self.get_prop(legacy_object, prop_name, index_or_id):
                    if prop in folio_object and mapped_prop not in folio_object.get(
                        prop, []
                    ):
                        folio_object.get(prop, []).append(mapped_prop)
                    else:
                        folio_object[prop] = [mapped_prop]
                self.report_legacy_mapping(
                    self.legacy_basic_property(prop_name), True, True
                )

    def map_basic_props(self, legacy_object, prop, folio_object, index_or_id):
        if self.has_basic_property(legacy_object, prop):  # is there a match in the csv?
            if mapped_prop := self.get_prop(legacy_object, prop, index_or_id):
                folio_object[prop] = mapped_prop
            self.report_legacy_mapping(self.legacy_basic_property(prop), True, True)

    def get_objects(self, source_file, file_name: Path):
        if str(file_name).endswith("tsv"):
            reader = csv.DictReader(source_file, dialect="tsv")
        else:
            reader = csv.DictReader(source_file)
        idx = 0
        try:
            for idx, row in enumerate(reader):
                yield row
        except Exception as exception:
            logging.error("%s at row %s", exception, idx)
            raise exception from exception

    def has_property(self, legacy_object, folio_prop_name: str):
        if not self.use_map:
            return folio_prop_name in legacy_object

        legacy_keys = self.field_map.get(folio_prop_name, [])
        return (
            any(legacy_keys)
            and any(k not in empty_vals for k in legacy_keys)
            and any(legacy_object.get(legacy_key, "") for legacy_key in legacy_keys)
        )

    def has_basic_property(self, legacy_object, folio_prop_name):
        if not self.use_map:
            return folio_prop_name in legacy_object

        if folio_prop_name not in self.folio_keys:
            return False
        legacy_keys = self.field_map.get(folio_prop_name, [])
        return (
            any(legacy_keys)
            and any(k not in empty_vals for k in legacy_keys)
            and any(legacy_object.get(legacy_key, "") for legacy_key in legacy_keys)
        )

    def legacy_basic_property(self, folio_prop):
        if not self.use_map:
            return folio_prop
        if folio_prop not in self.folio_keys:
            return ""
        return next(
            (
                k["legacy_field"]
                for k in self.record_map["data"]
                if k["folio_field"] == folio_prop
            ),
            "",
        )

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        if ref_object := self.ref_data_dicts.get(dict_key, {}).get(
            key_value.lower().strip(), ()
        ):
            return ref_object
        d = {r[key_type].lower(): (r["id"], r["name"]) for r in ref_data}
        self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ())


def skip_property(property_name_level1, property_level1):
    return bool(
        property_name_level1 in ["metadata", "id", "type", "lastCheckIn"]
        or property_name_level1.startswith("effective")
        or property_level1.get("folio:isVirtual", False)
    )


def weird_division(number, divisor):
    return number / divisor if divisor else 0
