import csv
import json
import logging
import sys
import uuid
from datetime import datetime
from typing import Dict

from dateutil.parser import parse
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapping_base_impl import (
    MappingFileMappingBaseImpl,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.mapping_file_transformation.user_mapper_base import (
    UserMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs


class UserMapper(UserMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        task_config,
        library_config,
        departments_mapping,
        groups_map,
    ):
        try:
            super().__init__(folio_client, library_config)
            self.noteprops = None
            self.notes_schemas = None
            self.notes_mapper = None
            self.task_config = task_config
            self.folio_keys = []
            self.library_config = library_config
            self.user_schema = FolioClient.get_latest_from_github(
                "folio-org", "mod-user-import", "/ramls/schemas/userdataimport.json"
            )
            self.ids_dict: Dict[str, set] = {}
            self.custom_props = {}
            # TODO: Use RefDataMapping
            if departments_mapping:
                self.departments_mapping = RefDataMapping(
                    self.folio_client,
                    "/departments",
                    "departments",
                    departments_mapping,
                    "name",
                    Blurbs.DepartmentsMapping,
                )
            else:
                self.departments_mapping = None
            if groups_map:
                self.groups_mapping = RefDataMapping(
                    self.folio_client,
                    "/groups",
                    "usergroups",
                    groups_map,
                    "group",
                    Blurbs.UserGroupMapping,
                )
            else:
                self.groups_mapping = None
            self.setup_notes_mapping()
            logging.info("Init done.")
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            print(f"\n{tpe.message}")
            sys.exit()

    def setup_notes_mapping(self):
        self.notes_schemas = FolioClient.get_latest_from_github(
            "folio-org",
            "mod-notes",
            "src/main/resources/swagger.api/schemas/note.yaml",
        )
        notes_common = FolioClient.get_latest_from_github(
            "folio-org",
            "mod-notes",
            "src/main/resources/swagger.api/schemas/common.yaml",
        )
        for prop in self.notes_schemas["note"]["properties"].items():
            if prop[1].get("$ref", "") == "common.yaml#/uuid":
                prop[1]["type"] = notes_common["uuid"]["type"]

        for p in ["links", "metadata", "id"]:
            del self.notes_schemas["note"]["properties"][p]

    def map_notes(self, user_map, legacy_user, legacy_id, user_uuid: str):
        if self.noteprops is None:
            self.noteprops = {
                "data": [
                    p for p in user_map["data"] if p["folio_field"].startswith("notes[")
                ]
            }
            logging.info(
                "Set %s props used for note mapping", len(self.noteprops["data"])
            )
        if any(self.noteprops["data"]):
            notes_schema = self.notes_schemas["noteCollection"]
            notes_schema["properties"]["notes"]["items"] = self.notes_schemas["note"]
            notes_schema["required"] = []
            if self.notes_mapper is None:
                self.notes_mapper = MappingFileMappingBaseImpl(
                    self.library_config,
                    self.folio_client,
                    notes_schema,
                    self.noteprops,
                    FOLIONamespaces.other,
                    True,
                )
                logging.info("Initiated mapper for User notes")
            for note in self.notes_mapper.do_map(
                legacy_user, legacy_id, FOLIONamespaces.other
            )[0].get("notes", []):
                if note.get("content", "").strip():
                    note["links"] = [{"id": user_uuid, "type": "user"}]

                    logging.log(25, "notes\t%s", json.dumps(note))
                    self.migration_report.add(Blurbs.MappedNoteTypes, note["typeId"])
                else:
                    self.migration_report.add_general_statistics(
                        "Notes without content that were discarded. Set some default "
                        "value if you only intend to set the note title"
                    )

    def do_map(self, legacy_user, user_map, legacy_id):
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            user_map
        )
        if not self.custom_props:
            for m in user_map["data"]:
                if "customFields" in m["folio_field"]:
                    sub_property = m["folio_field"].split(".")[-1]
                    self.custom_props[sub_property] = m["legacy_field"]
            logging.info(f"Found {len(self.custom_props)} Custom fields to be mapped.")
        # TODO: Create ID-Legacy ID Mapping file!
        # TODO: Check for ID duplicates (barcodes, externalsystemID:s, usernames, emails?)

        folio_user = self.instantiate_user(legacy_id)
        for prop_name, prop in self.user_schema["properties"].items():
            self.add_prop(legacy_user, user_map, folio_user, prop_name, prop)

        folio_user["personal"]["preferredContactTypeId"] = "Email"
        folio_user["active"] = True
        folio_user["requestPreference"] = {
            "userId": folio_user["id"],
            "holdShelf": True,
            "delivery": False,
            "metadata": self.folio_client.get_metadata_construct(),
        }
        self.map_notes(user_map, legacy_user, legacy_id, folio_user["id"])
        clean_folio_object = self.validate_required_properties(
            legacy_id, folio_user, self.user_schema, FOLIONamespaces.users
        )

        self.report_folio_mapping(clean_folio_object)
        self.report_legacy_mapping(legacy_user)
        return clean_folio_object

    def add_prop(self, legacy_object, user_map, folio_user, prop_name, prop):
        if prop["type"] == "object":
            if "customFields" in prop_name:
                for k, v in self.custom_props.items():
                    if legacy_value := legacy_object.get(v, ""):
                        folio_user["customFields"][k] = legacy_value
            else:
                folio_user[prop_name] = {}
                prop_key = prop_name
                if "properties" in prop:
                    for sub_prop_name, sub_prop in prop["properties"].items():
                        sub_prop_key = f"{prop_key}.{sub_prop_name}"
                        if "properties" in sub_prop:
                            for sub_prop_name2, sub_prop2 in sub_prop[
                                "properties"
                            ].items():
                                sub_prop_key2 = f"{sub_prop_key}.{sub_prop_name2}"
                                if sub_prop2["type"] == "array":
                                    logging.warning(f"Array: {sub_prop_key2} ")
                        elif sub_prop["type"] == "array":
                            folio_user[prop_name][sub_prop_name] = []
                            for i in range(5):
                                if sub_prop["items"]["type"] == "object":
                                    temp = {
                                        sub_prop_name2: self.get_prop(
                                            legacy_object,
                                            user_map,
                                            f"{sub_prop_key}.{sub_prop_name2}",
                                            i,
                                        )
                                        for sub_prop_name2, sub_prop2 in sub_prop[
                                            "items"
                                        ]["properties"].items()
                                    }

                                    if all(
                                        value == ""
                                        for key, value in temp.items()
                                        if key
                                        not in ["id", "primaryAddress", "addressTypeId"]
                                    ):
                                        continue
                                    folio_user[prop_name][sub_prop_name].append(temp)
                                else:
                                    mkey = f"{sub_prop_key}.{sub_prop_name2}"
                                    folio_user[prop_name][
                                        sub_prop_name
                                    ] = self.get_prop(legacy_object, mkey, i)

                        else:
                            folio_user[prop_name][sub_prop_name] = self.get_prop(
                                legacy_object, user_map, sub_prop_key
                            )
                if folio_user[prop_name] == {}:
                    del folio_user[prop_name]
        elif prop["type"] == "array":
            if prop["items"]["type"] == "string":
                prop_names = [p for p in self.folio_keys if p.startswith(prop_name)]
                for idx, arr_prop_name in enumerate(prop_names):
                    actual_prop_name = arr_prop_name.split("[")[0]
                    if any(folio_user.get(actual_prop_name, [])):
                        folio_user[actual_prop_name].append(
                            self.get_prop(
                                legacy_object, user_map, actual_prop_name, idx
                            )
                        )
                    else:
                        folio_user[actual_prop_name] = [
                            self.get_prop(
                                legacy_object, user_map, actual_prop_name, idx
                            )
                        ]
                if prop_name in folio_user and not any(folio_user.get(prop_name, [])):
                    del folio_user[prop_name]
            else:
                logging.info("Edge case %s", prop_name)
        else:
            self.map_basic_props(legacy_object, user_map, prop_name, folio_user)

    def map_basic_props(self, legacy_user, user_map, prop, folio_user):
        if self.has_property(legacy_user, user_map, prop):
            if temp_prop := self.get_prop(legacy_user, user_map, prop).strip():
                folio_user[prop] = temp_prop

    def add_notes(self):
        note = {
            "id": str(uuid.uuid4()),
            "typeId": "e00f14d9-001e-4084-be04-961c0ed4b2a6",
            "type": "Check in",
            "title": "Note title",
            "domain": "users",
            "content": "<p>Wow! WYSIWYG! <strong>Bold</strong> move!</p>",
            "popUpOnCheckOut": True,
            "popUpOnUser": True,
            "links": [{"id": "c0901dc0-b668-4bd3-8e73-35eb45a07665", "type": "user"}],
            "metadata": self.folio_client.get_metadata_construct(),
        }

    def get_users(self, source_file, file_format: str):
        csv.register_dialect("tsv", delimiter="\t")
        if file_format == "tsv":
            reader = csv.DictReader(source_file, dialect="tsv")
        else:  # Assume csv
            reader = csv.DictReader(source_file)
        for idx, row in enumerate(reader):
            if len(row.keys()) < 3:
                raise TransformationProcessError(
                    idx, "something is wrong source file row", json.dumps(row)
                )
            yield row

    def get_prop(self, legacy_user, user_map, folio_prop_name, i=0):
        legacy_user_key = get_legacy_user_key(folio_prop_name, user_map["data"], i)
        value = get_legacy__user_value(folio_prop_name, user_map["data"], i)

        # The value is set on the mapping. Return this instead of the default field
        if value:
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value

        if folio_prop_name == "personal.addresses.id":
            return "not needed"
        elif folio_prop_name.split("[")[0] == "departments":
            legacy_dept = legacy_user.get(legacy_user_key, "")
            gid = self.get_mapped_name(
                self.departments_mapping,
                legacy_user,
                id,
                folio_prop_name.split("[")[0],
                True,
            )
            self.migration_report.add(
                Blurbs.DepartmentsMapping, f"{legacy_dept} -> {gid}"
            )
            return gid
        elif folio_prop_name == "patronGroup":
            legacy_group = legacy_user.get(legacy_user_key, "")
            if self.groups_mapping:
                gid = self.get_mapped_name(
                    self.groups_mapping,
                    legacy_user,
                    id,
                    "",
                    True,
                )
                return gid
            else:
                self.migration_report.add(
                    Blurbs.UserGroupMapping,
                    f"{legacy_group} -> {legacy_group} (one to one)",
                )
                self.migration_report.add(Blurbs.UsersPerPatronType, legacy_group)
                return legacy_group
        elif folio_prop_name in ["expirationDate", "enrollmentDate"]:
            try:
                format_date = parse(legacy_user.get(legacy_user_key), fuzzy=True)
                return format_date.isoformat()
            except Exception as ee:
                v = legacy_user.get(legacy_user_key)
                logging.error(f"expiration date {v} could not be parsed: {ee}")
                return datetime.utcnow().isoformat()
        elif folio_prop_name.strip() == "personal.addresses.primaryAddress":
            return value
        elif folio_prop_name == "personal.addresses.addressTypeId":
            try:
                return user_map["addressTypes"][i]
            except (KeyError, IndexError):
                return ""
        elif legacy_user_key:
            return legacy_user.get(legacy_user_key, "")
        else:
            return ""

    def has_property(self, user, user_map, folio_prop_name):
        user_key = next(
            (
                k["legacy_field"]
                for k in user_map["data"]
                if k["folio_field"].split("[")[0] == folio_prop_name
            ),
            "",
        )
        return (
            user_key and user_key not in ["", "Not mapped"] and user.get(user_key, "")
        )

    def legacy_property(self, user_map, folio_prop_name):
        if value := next(
            (
                k.get("value", "")
                for k in user_map["data"]
                if k["folio_field"] == folio_prop_name
            ),
            "",
        ):
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value
        return next(
            k["legacy_field"]
            for k in user_map["data"]
            if k["folio_field"] == folio_prop_name
        )


def get_legacy__user_value(folio_prop_name, data, i):
    return next(
        (
            k.get("value", "")
            for k in data
            if k["folio_field"].replace(f"[{i}]", "") == folio_prop_name
            or k["folio_field"] == folio_prop_name
        ),
        "",
    )


def get_legacy_user_key(folio_prop_name, data, i):
    return next(
        (
            k["legacy_field"]
            for k in data
            if k["folio_field"].replace(f"[{i}]", "") == folio_prop_name
        ),
        "",
    )
