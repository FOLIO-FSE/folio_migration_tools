import csv
import json
import logging
import sys
from datetime import datetime
from typing import Dict

from dateutil.parser import parse
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


class UserMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        task_config,
        library_config,
        user_map,
        departments_mapping,
        groups_map,
    ):
        try:
            user_schema = FolioClient.get_latest_from_github(
                "folio-org", "mod-user-import", "/ramls/schemas/userdataimport.json"
            )
            super().__init__(
                folio_client,
                user_schema,
                user_map,
                None,
                FOLIONamespaces.users,
                library_config,
            )
            self.task_config = task_config
            self.notes_mapper: NotesMapper = NotesMapper(
                self.library_configuration,
                self.folio_client,
                self.record_map,
                FOLIONamespaces.users,
                True,
            )
            self.notes_mapper.migration_report = self.migration_report
            self.custom_props: Dict = {}
            self.setup_departments_mapping(departments_mapping)
            self.setup_groups_mapping(groups_map)
            logging.info("Init done.")
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            print(f"\n{tpe.message}\t{tpe.data_value}")
            sys.exit(1)

    def do_map_old(self, legacy_user, legacy_id):
        missing_keys_in_user = [f for f in self.mapped_legacy_keys if f not in legacy_user]
        if any(missing_keys_in_user):
            raise TransformationProcessError(
                "",
                ("There are mapped legacy fields that are not in the legacy user record"),
                missing_keys_in_user,
            )

        if not self.custom_props:
            for m in self.record_map["data"]:
                if "customFields" in m["folio_field"]:
                    sub_property = m["folio_field"].split(".")[-1]
                    self.custom_props[sub_property] = m["legacy_field"]

        folio_user = self.instantiate_user(legacy_id)
        for prop_name, prop in self.schema["properties"].items():
            self.add_prop(legacy_user, self.record_map, folio_user, prop_name, prop)

        folio_user["personal"]["preferredContactTypeId"] = "Email"
        folio_user["active"] = True
        folio_user["requestPreference"] = {
            "userId": folio_user["id"],
            "holdShelf": True,
            "delivery": False,
            "metadata": self.folio_client.get_metadata_construct(),
        }

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
                            for sub_prop_name2, sub_prop2 in sub_prop["properties"].items():
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
                                        for sub_prop_name2, sub_prop2 in sub_prop["items"][
                                            "properties"
                                        ].items()
                                    }

                                    if all(
                                        value == ""
                                        for key, value in temp.items()
                                        if key not in ["id", "primaryAddress", "addressTypeId"]
                                    ):
                                        continue
                                    folio_user[prop_name][sub_prop_name].append(temp)
                                else:
                                    mkey = f"{sub_prop_key}.{sub_prop_name2}"
                                    folio_user[prop_name][sub_prop_name] = self.get_prop(
                                        legacy_object, mkey, i
                                    )

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
                            self.get_prop(legacy_object, user_map, actual_prop_name, idx)
                        )
                    else:
                        folio_user[actual_prop_name] = [
                            self.get_prop(legacy_object, user_map, actual_prop_name, idx)
                        ]
                if prop_name in folio_user and not any(folio_user.get(prop_name, [])):
                    del folio_user[prop_name]
            else:
                logging.info("Edge case %s", prop_name)
        else:
            self.map_basic_props(legacy_object, user_map, prop_name, folio_user)

    def map_basic_props_old(self, legacy_user, user_map, prop, folio_user):
        if self.has_property(legacy_user, user_map, prop):
            if temp_prop := self.get_prop(legacy_user, user_map, prop).strip():
                folio_user[prop] = temp_prop

    def perform_additional_mapping(self, legacy_user, folio_user, index_or_id):
        self.notes_mapper.map_notes(
            legacy_user, index_or_id, folio_user["id"], FOLIONamespaces.users
        )
        if "personal" not in folio_user:
            folio_user["personal"] = {}
        folio_user["personal"]["preferredContactTypeId"] = "Email"
        folio_user["active"] = True
        folio_user["requestPreference"] = {
            "userId": folio_user["id"],
            "holdShelf": True,
            "delivery": False,
            "metadata": self.folio_client.get_metadata_construct(),
        }
        clean_folio_object = self.validate_required_properties(
            index_or_id, folio_user, self.schema, FOLIONamespaces.users
        )

        if "preferredFirstName" in clean_folio_object.get(
            "personal", {}
        ) and not clean_folio_object.get("personal", {}).get("preferredFirstName", ""):
            del clean_folio_object["personal"]["preferredFirstName"]
        self.report_folio_mapping_no_schema(clean_folio_object)
        self.report_legacy_mapping_no_schema(legacy_user)

        return clean_folio_object

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

    def get_prop(self, legacy_user, folio_prop_name, index_or_id):
        value_tuple = (legacy_user, folio_prop_name, index_or_id)
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        lm = list(
            MappingFileMapperBase.get_legacy_user_mappings(
                folio_prop_name, self.record_map["data"]
            )
        )
        if folio_prop_name in self.mapped_from_values and len(legacy_item_keys) == 1:
            return self.mapped_from_values.get(folio_prop_name, "")

        elif folio_prop_name == "personal.addresses.id":
            return ""
        elif folio_prop_name == "patronGroup":
            if self.groups_mapping:
                return self.get_mapped_name(
                    self.groups_mapping,
                    *value_tuple,
                    False,
                )
            else:
                return MappingFileMapperBase.get_legacy_value(
                    legacy_user, lm[0], self.migration_report
                )
        elif folio_prop_name.startswith("departments"):
            if not self.departments_mapping:
                raise TransformationProcessError(
                    "",
                    "No Departments mapping set up. Set up a departments mapping file "
                    " or remove the mapping of the Departments field",
                )
            return self.get_mapped_name(
                self.departments_mapping,
                *value_tuple,
                False,
            )
        elif any(lm) and folio_prop_name in [
            "expirationDate",
            "enrollmentDate",
            "personal.dateOfBirth",
        ]:
            return self.get_parsed_date(legacy_user, lm[0], folio_prop_name)

        if len(lm) > 1:
            self.migration_report.add(Blurbs.Details, f"{legacy_item_keys} were concatenated")
        return " ".join(
            MappingFileMapperBase.get_legacy_value(legacy_user, m, self.migration_report)
            for m in lm
        ).strip()

    def get_parsed_date(self, legacy_user: dict, legacy_mapping: dict, folio_prop_name: str):
        try:
            if not self.get_legacy_value(legacy_user, legacy_mapping, self.migration_report):
                return ""
            format_date = parse(
                self.get_legacy_value(legacy_user, legacy_mapping, self.migration_report),
                fuzzy=True,
            )
            fmt_string = (
                f"{folio_prop_name}: "
                f"{self.get_legacy_value(legacy_user, legacy_mapping,self.migration_report)}"
                f" -> {format_date.isoformat()}"
            )
            self.migration_report.add(Blurbs.DateTimeConversions, fmt_string)
            return format_date.isoformat()
        except Exception as ee:
            v = self.get_legacy_value(legacy_user, legacy_mapping, self.migration_report)
            logging.error(f"{folio_prop_name} {v} could not be parsed: {ee}")
            fmt_string = f"Parsing error! {folio_prop_name}: {v}. NOW() was returned"
            self.migration_report.add(Blurbs.DateTimeConversions, fmt_string)
            return datetime.utcnow().isoformat()

    def get_prop_old(self, legacy_user, user_map, folio_prop_name, index_or_id):
        # value_tuple = (legacy_user, folio_prop_name, index_or_id)
        # The value is set on the mapping. Return this instead of the default field

        # All other cases are mapped from legacy fields.
        legacy_user_mappings = list(
            MappingFileMapperBase.get_legacy_user_mappings(folio_prop_name, user_map["data"])
        )
        if not any(legacy_user_mappings):
            return ""

        legacy_user_mapping = legacy_user_mappings[0]

        if folio_prop_name.split("[")[0] == "departments":
            if not self.departments_mapping:
                raise TransformationProcessError(
                    "",
                    "No Departments mapping set up. Set up a departments mapping file "
                    " or remove the mapping of the Departments field",
                )
            legacy_dept = self.get_legacy_value(legacy_user, legacy_user_mapping)
            gid = self.get_mapped_name(
                self.departments_mapping,
                legacy_user,
                id,
                folio_prop_name.split("[")[0],
                True,
            )
            self.migration_report.add(Blurbs.DepartmentsMapping, f"{legacy_dept} -> {gid}")
            return gid
        elif folio_prop_name == "patronGroup":
            legacy_group = self.get_legacy_value(legacy_user, legacy_user_mapping)
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
        elif legacy_user_mappings:
            if len(legacy_user_mappings) > 1:
                self.migration_report.add(
                    Blurbs.Details, f"{legacy_user_mappings} concatenated into one string"
                )
            return " ".join(
                self.get_legacy_value(legacy_user, mapping) for mapping in legacy_user_mappings
            ).strip()
        else:
            return ""

    def has_property_old(self, user, user_map, folio_prop_name):
        user_mapping = next(
            (k for k in user_map["data"] if k["folio_field"].split("[")[0] == folio_prop_name),
            "",
        )
        return (
            user_mapping
            and user_mapping["legacy_field"] not in ["", "Not mapped"]
            and self.get_legacy_value(user, user_mapping)
        )

    def legacy_property(self, user_map, folio_prop_name):
        if value := next(
            (k.get("value", "") for k in user_map["data"] if k["folio_field"] == folio_prop_name),
            "",
        ):
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value
        return next(
            k["legacy_field"] for k in user_map["data"] if k["folio_field"] == folio_prop_name
        )

    def setup_groups_mapping(self, groups_map):
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

    def setup_departments_mapping(self, departments_mapping):
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


def get_value_from_map(folio_prop_name, data, i):
    return next(
        (
            k.get("value", "")
            for k in data
            if k["folio_field"].replace(f"[{i}]", "") == folio_prop_name
            or k["folio_field"] == folio_prop_name
        ),
        "",
    )
