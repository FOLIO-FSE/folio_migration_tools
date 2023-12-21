import csv
import json
import logging
import sys
import i18n

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
            user_schema = folio_client.get_from_github(
                "folio-org", "mod-user-import", "/ramls/schemas/userdataimport.json"
            )

            user_schema["properties"]["requestPreference"] = folio_client.get_from_github(
                "folio-org", "mod-user-import", "/ramls/schemas/userImportRequestPreference.json"
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
        if folio_user.get("requestPreference"):
            folio_user["requestPreference"].update(
                {
                    "holdShelf": True,
                    "delivery": False,
                }
            )
        else:
            folio_user["requestPreference"] = {
                "holdShelf": True,
                "delivery": False,
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

        if self.task_config.remove_request_preferences:
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

    def get_prop(self, legacy_user, folio_prop_name, index_or_id, schema_default_value):
        mapped_value = super().get_prop(
            legacy_user, folio_prop_name, index_or_id, schema_default_value
        )
        if folio_prop_name == "personal.addresses.id":
            return ""
        elif folio_prop_name == "patronGroup":
            if self.groups_mapping:
                return self.get_mapped_name(
                    self.groups_mapping,
                    legacy_user,
                    index_or_id,
                    False,
                )
            else:
                return mapped_value
        elif folio_prop_name.startswith("departments"):
            if not self.departments_mapping:
                raise TransformationProcessError(
                    "",
                    "No Departments mapping set up. Set up a departments mapping file "
                    " or remove the mapping of the Departments field",
                )
            return self.get_mapped_name(
                self.departments_mapping,
                legacy_user,
                index_or_id,
                False,
            )
        elif folio_prop_name in ["expirationDate", "enrollmentDate", "personal.dateOfBirth"]:
            return self.get_parsed_date(mapped_value, folio_prop_name)
        return mapped_value

    def get_parsed_date(self, mapped_value, folio_prop_name: str):
        try:
            if not mapped_value.strip():
                return ""
            format_date = parse(mapped_value, fuzzy=True)
            return format_date.isoformat()
        except Exception as ee:
            v = mapped_value
            logging.error(f"{folio_prop_name} {v} could not be parsed: {ee}")
            fmt_string = i18n.t(
                "Parsing error! %{prop_name}: %{value}. The empty string was returned",
                prop_name=folio_prop_name,
                value=v,
            )
            self.migration_report.add("DateTimeConversions", fmt_string)
            return ""

    def setup_groups_mapping(self, groups_map):
        if groups_map:
            self.groups_mapping = RefDataMapping(
                self.folio_client,
                "/groups",
                "usergroups",
                groups_map,
                "group",
                "UserGroupMapping",
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
                "DepartmentsMapping",
            )
        else:
            self.departments_mapping = None
