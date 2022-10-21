import csv
import json
import logging
import sys
from datetime import datetime

from dateutil.parser import parse
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
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
            self.setup_departments_mapping(departments_mapping)
            self.setup_groups_mapping(groups_map)

            for m in self.record_map["data"]:
                if m["folio_field"].startswith("customFields"):
                    if "properties" not in self.schema["properties"]["customFields"]:
                        self.schema["properties"]["customFields"]["properties"] = {}
                    custom_field_prop_name = m["folio_field"].split(".")[-1]
                    self.schema["properties"]["customFields"]["properties"][
                        custom_field_prop_name
                    ] = {"type": "string", "description": "dynamically added custom prop"}

            logging.info("Init done.")
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            print(f"\n{tpe.message}\t{tpe.data_value}")
            sys.exit(1)

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
        if not clean_folio_object.get("personal", {}).get("lastName", ""):
            raise TransformationRecordFailedError(index_or_id, "Last name is missing", "")

        if "preferredFirstName" in clean_folio_object.get(
            "personal", {}
        ) and not clean_folio_object.get("personal", {}).get("preferredFirstName", ""):
            del clean_folio_object["personal"]["preferredFirstName"]

        if self.task_config.remove_id_and_request_preferences:
            del clean_folio_object["id"]
            del clean_folio_object["requestPreference"]
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
        map_entries = list(
            MappingFileMapperBase.get_map_entries_by_folio_prop_name(
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
                    legacy_user, map_entries[0], self.migration_report
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
        elif any(map_entries) and folio_prop_name in [
            "expirationDate",
            "enrollmentDate",
            "personal.dateOfBirth",
        ]:
            return self.get_parsed_date(legacy_user, map_entries[0], folio_prop_name)

        if len(map_entries) > 1:
            self.migration_report.add(Blurbs.Details, f"{legacy_item_keys} were concatenated")
        return " ".join(
            MappingFileMapperBase.get_legacy_value(legacy_user, map_entry, self.migration_report)
            for map_entry in map_entries
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
