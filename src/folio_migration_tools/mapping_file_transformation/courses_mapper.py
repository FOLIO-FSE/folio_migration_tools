from typing import Any
from typing import Dict

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
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
        library_configuration: LibraryConfiguration,
    ):
        self.composite_course_schema = self.get_composite_course_schema()
        super().__init__(
            folio_client,
            self.composite_course_schema,
            course_map,
            None,
            FOLIONamespaces.course,
            library_configuration,
        )
        self.course_map = course_map
        self.terms_map = terms_map
        self.ids_dict: Dict[str, set] = {}
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
        self.setup_notes_mapping()

    def do_map(self):
        raise NotImplementedError()

    def perform_additional_mappings(self):
        raise NotImplementedError()

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
        composite_map = {
            "course": FolioClient.get_latest_from_github(
                "folio-org", "mod-courses", "/ramls/course.json"
            ),
            "courselisting": FolioClient.get_latest_from_github(
                "folio-org", "mod-courses", "/ramls/courselisting.json"
            ),
            "instructor": FolioClient.get_latest_from_github(
                "folio-org", "mod-courses", "/ramls/instructor.json"
            ),
            "notes": CoursesMapper.get_notes_schema()["noteCollection"],
        }
        composite_map["notes"]["items"] = CoursesMapper.get_notes_schema()["note"]
        return composite_map
