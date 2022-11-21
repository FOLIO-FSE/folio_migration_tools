from typing import Any
from typing import Dict

from folio_uuid.folio_uuid import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


class CoursesMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        course_map,
        terms_map,
        departments_map,
        library_configuration: LibraryConfiguration,
        task_configuration,
    ):
        self.folio_client = folio_client
        self.user_cache: dict = {}
        self.notes_mapper: NotesMapper = NotesMapper(
            library_configuration,
            self.folio_client,
            course_map,
            FOLIONamespaces.note,
            True,
        )
        self.composite_course_schema = self.get_composite_course_schema()
        self.task_configuration = task_configuration
        super().__init__(
            folio_client,
            self.composite_course_schema,
            course_map,
            None,
            FOLIONamespaces.course,
            library_configuration,
        )
        self.course_map = course_map
        self.use_map = True
        if terms_map:
            self.terms_map = RefDataMapping(
                self.folio_client,
                "/coursereserves/terms",
                "terms",
                terms_map,
                "name",
                Blurbs.TermsMapping,
            )
        else:
            self.terms_map = None

        if departments_map:
            self.departments_map = RefDataMapping(
                self.folio_client,
                "/coursereserves/departments",
                "departments",
                departments_map,
                "name",
                Blurbs.DepartmentsMapping,
            )
        else:
            self.departments_map = None

    def store_objects(self, composite_course):
        try:
            self.extradata_writer.write("courselisting", composite_course[0]["courselisting"])
            self.migration_report.add_general_statistics("Stored courselistings")
            self.extradata_writer.write("course", composite_course[0]["course"])
            self.migration_report.add_general_statistics("Stored courses")
            if "instructors" in composite_course[0] and any(composite_course[0]["instructors"]):
                for instructor in composite_course[0]["instructors"]:
                    self.extradata_writer.write("instructor", instructor)
                    self.migration_report.add_general_statistics("Stored instructors")

        except Exception as ee:
            raise TransformationRecordFailedError(
                composite_course[1], "Failed when storing", ee
            ) from ee

    def perform_additional_mappings(self, composite_course):
        try:
            # Assign deterministic ids to every object
            composite_course[0]["course"]["id"] = self.get_uuid(
                composite_course, FOLIONamespaces.course
            )
            composite_course[0]["courselisting"]["id"] = self.get_uuid(
                composite_course, FOLIONamespaces.course_listing
            )
            if "instructors" in composite_course[0] and any(composite_course[0]["instructors"]):
                for idx, instructor in enumerate(composite_course[0]["instructors"]):
                    instructor["id"] = self.get_uuid(
                        composite_course, FOLIONamespaces.instructor, idx
                    )
                    # Link instructor to course listing
                    instructor["courseListingId"] = composite_course[0]["courselisting"]["id"]
                    if self.task_configuration.look_up_instructor:
                        self.populate_instructor_from_users(instructor)
            else:
                self.migration_report.add_general_statistics("Missing Instructors")

            # Link course to courselisting
            composite_course[0]["course"]["courseListingId"] = composite_course[0][
                "courselisting"
            ]["id"]

        except Exception as ee:
            raise TransformationRecordFailedError(
                composite_course[1], "Failed when creating and linking ids", ee
            ) from ee

    def get_uuid(self, composite_course, object_type: FOLIONamespaces, idx: int = 0):
        return str(
            FolioUUID(
                self.folio_client.okapi_url,
                object_type,
                composite_course[1] if idx == 0 else f"{composite_course[1]}_{idx}",
            )
        )

    def populate_instructor_from_users(self, instructor: dict):
        if instructor["userId"] not in self.user_cache:
            path = "/users"
            query = f'?query=(externalSystemId=="{instructor["userId"]}")'
            if user := next(self.folio_client.folio_get_all(path, "users", query), None):
                self.user_cache[instructor["userId"]] = user
        if user := self.user_cache.get(instructor["userId"], {}):
            instructor["userId"] = user.get("id", "")
            instructor["barcode"] = user.get("barcode", "")
            instructor["patronGroup"] = user.get("patronGroup", "")
        else:
            del instructor["userId"]

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        value_tuple = (legacy_item, folio_prop_name, index_or_id)

        # Legacy contstruct
        if not self.use_map:
            return legacy_item[folio_prop_name]

        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])

        # IF there is a value mapped, return that one
        if len(legacy_item_keys) == 1 and folio_prop_name in self.mapped_from_values:
            value = self.mapped_from_values.get(folio_prop_name, "")
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value

        legacy_values = self.get_legacy_vals(legacy_item, legacy_item_keys)
        legacy_value = " ".join(legacy_values).strip()

        if folio_prop_name == "courselisting.termId":
            return self.get_mapped_value(
                self.terms_map,
                *value_tuple,
                False,
            )
        elif folio_prop_name == "course.departmentId":
            return self.get_mapped_value(
                self.departments_map,
                *value_tuple,
                False,
            )
        elif any(legacy_item_keys):
            if len(legacy_item_keys) > 1:
                self.migration_report.add(Blurbs.Details, f"{legacy_item_keys} were concatenated")
            return legacy_value
        else:
            self.migration_report.add(
                Blurbs.UnmappedProperties, f"{folio_prop_name} {legacy_item_keys}"
            )
            return ""

    @staticmethod
    def get_composite_course_schema() -> Dict[str, Any]:
        return {
            "properties": {
                "course": FolioClient.get_latest_from_github(
                    "folio-org", "mod-courses", "/ramls/course.json"
                ),
                "courselisting": FolioClient.get_latest_from_github(
                    "folio-org", "mod-courses", "/ramls/courselisting.json"
                ),
                "instructors": {
                    "type": "array",
                    "items": FolioClient.get_latest_from_github(
                        "folio-org", "mod-courses", "/ramls/instructor.json"
                    ),
                },
            }
        }
