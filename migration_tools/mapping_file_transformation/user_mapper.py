import csv
import json
import logging
from datetime import datetime
from typing import Dict

from dateutil.parser import parse
from folioclient import FolioClient
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.mapping_file_transformation.user_mapper_base import UserMapperBase


class UserMapper(UserMapperBase):
    def __init__(
        self, folio_client: FolioClient, task_config, library_config, groups_map=None
    ):
        super().__init__(folio_client)
        if groups_map is None:
            groups_map = []
        self.task_config = task_config
        self.library_config = library_config
        self.groups_map = {}
        self.user_schema = UserMapperBase.get_latest_from_github(
            "folio-org", "mod-user-import", "/ramls/schemas/userdataimport.json"
        )
        self.ids_dict: Dict[str, set] = {}
        self.custom_props = {}
        folio_group_names = [
            g["group"] for g in list(self.folio_client.get_all("/groups", "usergroups"))
        ]
        logging.info(f"Fetched {len(folio_group_names)} groups from FOLIO")
        print(json.dumps(groups_map, indent=4))
        for m in groups_map:
            if m["folio_name"] in folio_group_names:
                self.groups_map[m["legacy_code"]] = m["folio_name"]
            else:
                raise TransformationProcessError(
                    f'FOLIO name {m["folio_name"]} from map is not in FOLIO '
                )

    def do_map(self, legacy_user, user_map, idx):
        if not self.custom_props:
            for m in user_map["data"]:
                if "customFields" in m["folio_field"]:
                    sub_property = m["folio_field"].split(".")[-1]
                    self.custom_props[sub_property] = m["legacy_field"]
            logging.debug(f"Found {len(self.custom_props)} Custom fields to be mapped.")

        # TODO: Create ID-Legacy ID Mapping file!
        # TODO: Check for ID duplicates (barcodes, externalsystemID:s, usernames, emails?)
        folio_user = self.instantiate_user()
        for prop_name, prop in self.user_schema["properties"].items():
            if prop.get("description", "") == "Deprecated":
                continue
            if prop_name == "metadata":
                continue
            if prop["type"] == "object":
                if "customFields" in prop_name:
                    for k, v in self.custom_props.items():
                        legacy_value = legacy_user.get(v, "")
                        if legacy_value:
                            folio_user["customFields"][k] = legacy_value
                    continue
                folio_user[prop_name] = {}
                prop_key = prop_name
                if "properties" in prop:
                    for sub_prop_name, sub_prop in prop["properties"].items():
                        sub_prop_key = prop_key + "." + sub_prop_name
                        if "properties" in sub_prop:
                            for sub_prop_name2, sub_prop2 in sub_prop[
                                "properties"
                            ].items():
                                sub_prop_key2 = sub_prop_key + "." + sub_prop_name2
                                if sub_prop2["type"] == "array":
                                    logging.warning(f"Array: {sub_prop_key2} ")
                        elif sub_prop["type"] == "array":
                            folio_user[prop_name][sub_prop_name] = []
                            for i in range(0, 5):
                                if sub_prop["items"]["type"] == "object":
                                    temp = {}
                                    for sub_prop_name2, sub_prop2 in sub_prop["items"][
                                        "properties"
                                    ].items():
                                        temp[sub_prop_name2] = self.get_prop(
                                            legacy_user,
                                            user_map,
                                            sub_prop_key + "." + sub_prop_name2,
                                            i,
                                        )
                                    if all(
                                        value == ""
                                        for key, value in temp.items()
                                        if key
                                        not in ["id", "primaryAddress", "addressTypeId"]
                                    ):
                                        continue
                                    folio_user[prop_name][sub_prop_name].append(temp)
                                else:
                                    mkey = sub_prop_key + "." + sub_prop_name2
                                    folio_user[prop_name][
                                        sub_prop_name
                                    ] = self.get_prop(legacy_user, mkey, i)

                        else:
                            folio_user[prop_name][sub_prop_name] = self.get_prop(
                                legacy_user, user_map, sub_prop_key
                            )

            elif prop["type"] == "array":
                """if prop["items"]["type"] == "object":
                    self.map_objects_array_props(
                        legacy_object,
                        prop_name,
                        prop["items"]["properties"],
                        folio_object,
                        index_or_id,
                    )
                elif prop["items"]["type"] == "string":
                    self.map_string_array_props(
                        legacy_object, prop_name, folio_object, index_or_id
                    )
                else:
                """
                # handle departments
                self.migration_report.add("Unhandled array", prop_name)
            else:
                self.map_basic_props(legacy_user, user_map, prop_name, folio_user)
                """ elif prop == "customFields":
                    custom_fields = [c for c in legacy_user if c.startswith("customField")]
                    if any(custom_fields):
                        for custom_field in custom_fields:
                            folio_user["customFields"][custom_field.split('.')[1]] = legacy_user[custom_field]
                            self.add_to_migration_report("General",
                                                         f"Custom field {custom_field.split('.')[1]} added")
                            """

            """ for prop in legacy_user:
            if prop not in mapped_legacy_props:
               """

        folio_user["personal"]["preferredContactTypeId"] = "Email"
        folio_user["active"] = True
        folio_user["requestPreference"] = {
            "userId": folio_user["id"],
            "holdShelf": True,
            "delivery": False,
            "metadata": self.folio_client.get_metadata_construct(),
        }
        required = self.user_schema["required"]
        for required_prop in required:
            if required_prop not in folio_user:
                print(json.dumps(folio_user))
                raise TransformationRecordFailedError(
                    f"Required property {required_prop} missing for \"{folio_user.get('barcode', '')}\" (barcode) {idx} (index in file)"
                )
            elif not folio_user[required_prop]:
                raise TransformationRecordFailedError(
                    f"Required property {required_prop} empty for \"{folio_user.get('barcode', '')}\" (barcode) {idx} (index in file)"
                )
        del folio_user["tags"]
        self.report_folio_mapping(folio_user)
        self.report_legacy_mapping(legacy_user)
        return folio_user

    def map_basic_props(self, legacy_user, user_map, prop, folio_user):
        if (
            self.has_property(legacy_user, user_map, prop)
            and self.get_prop(legacy_user, user_map, prop).strip()
        ):
            folio_user[prop] = self.get_prop(legacy_user, user_map, prop)

    def get_users(self, source_file, file_format: str):
        csv.register_dialect("tsv", delimiter="\t")
        if file_format == "tsv":
            reader = csv.DictReader(source_file, dialect="tsv")
        else:  # Assume csv
            reader = csv.DictReader(source_file)
        for idx, row in enumerate(reader):
            if len(row.keys()) < 3:
                raise TransformationRecordFailedError(
                    idx, "something is wrong source file row", json.dumps(row)
                )
            yield row

    def get_prop(self, legacy_user, user_map, folio_prop_name, i=0):
        legacy_user_key = get_legacy_user_key(folio_prop_name, user_map["data"], i)
        value = get_legacy__user_value(folio_prop_name, user_map["data"], i)
        # The value is set on the mapping. Return this instead of the default field
        if value:
            self.migration_report.add(
                "Default values added", f"{value} added to {folio_prop_name}"
            )
            return value

        if folio_prop_name == "personal.addresses.id":
            return "not needed"
        elif folio_prop_name == "patronGroup":
            legacy_group = legacy_user.get(legacy_user_key, "")
            if self.task_config.use_group_map:
                mapped_legacy_group = self.groups_map.get(legacy_group, "")
                if not mapped_legacy_group:
                    logging.fatal(
                        f"Patron group {legacy_group} not in groups map!. Halting"
                    )
                    exit()
                self.migration_report.add(
                    "User group mapping", f"{legacy_group} -> {mapped_legacy_group}"
                )
                return self.groups_map[legacy_group]
            else:
                self.migration_report.add(
                    "User group mapping",
                    f"{legacy_group} -> {legacy_group} (one to one)",
                )
                self.migration_report.add("Users per patron type", legacy_group)
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
                address_type_id = user_map["addressTypes"][i]
                return address_type_id
            except KeyError:
                return ""
            except IndexError:
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
                if k["folio_field"] == folio_prop_name
            ),
            "",
        )
        return (
            user_key and user_key not in ["", "Not mapped"] and user.get(user_key, "")
        )

    def legacy_property(self, user_map, folio_prop_name):
        value = next(
            (
                k.get("value", "")
                for k in user_map["data"]
                if k["folio_field"] == folio_prop_name
            ),
            "",
        )
        if value:
            self.migration_report.add(
                "Default values added", f"{value} added to {folio_prop_name}"
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
