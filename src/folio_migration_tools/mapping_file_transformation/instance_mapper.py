"""Mapper for transforming delimited instance data to FOLIO Instance format.

Provides the InstanceMapper class for mapping legacy delimited (CSV/TSV) bibliographic
data to FOLIO Instance records using a mapping file. Handles instance types, nature of
content, formats, modes of issuance and contributor types via reference data maps, and
identifier types, contributor name types and instance note types by FOLIO name or UUID.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.utils import normalize_for_compare

if TYPE_CHECKING:
    from folio_migration_tools.migration_tasks.instance_csv_transformer import (
        InstanceCsvTransformer,
    )

logger = logging.getLogger(__name__)


class InstanceMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        instance_map,
        instance_type_map,
        library_configuration: LibraryConfiguration,
        task_config: InstanceCsvTransformer.TaskConfiguration,
        statistical_codes_map=None,
        nature_of_content_map=None,
        instance_format_map=None,
        mode_of_issuance_map=None,
        contributor_type_map=None,
        identifier_type_map=None,
        contributor_name_type_map=None,
        instance_note_type_map=None,
    ):
        """Initialize InstanceMapper for delimited instance transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            instance_map: Mapping configuration for instance fields.
            instance_type_map: Mapping of legacy values to FOLIO instance types (required).
            library_configuration (LibraryConfiguration): Library configuration.
            task_config: Task configuration.
            statistical_codes_map: Mapping of legacy to FOLIO statistical codes.
            nature_of_content_map: Mapping of legacy to FOLIO nature of content terms.
            instance_format_map: Mapping of legacy to FOLIO instance formats.
            mode_of_issuance_map: Mapping of legacy to FOLIO modes of issuance.
            contributor_type_map: Mapping of legacy to FOLIO contributor types.
            identifier_type_map: Optional mapping of legacy to FOLIO identifier types.
            contributor_name_type_map: Optional mapping of legacy to FOLIO contributor
                name types.
            instance_note_type_map: Optional mapping of legacy to FOLIO instance note types.
        """
        instance_schema = folio_client.get_instance_json_schema()
        super().__init__(
            folio_client,
            instance_schema,
            instance_map,
            statistical_codes_map,
            FOLIONamespaces.instances,
            library_configuration,
            task_config,
        )
        self.instance_map = instance_map
        self.instance_type_mapping = RefDataMapping(
            self.folio_client,
            "/instance-types",
            "instanceTypes",
            instance_type_map,
            "name",
            "InstanceTypeMapping",
        )
        self.nature_of_content_mapping = self._optional_ref_data_mapping(
            "/nature-of-content-terms",
            "natureOfContentTerms",
            nature_of_content_map,
            "NatureOfContentMapping",
        )
        self.instance_format_mapping = self._optional_ref_data_mapping(
            "/instance-formats",
            "instanceFormats",
            instance_format_map,
            "InstanceFormatMapping",
        )
        self.mode_of_issuance_mapping = self._optional_ref_data_mapping(
            "/modes-of-issuance",
            "issuanceModes",
            mode_of_issuance_map,
            "ModeOfIssuanceMapping",
        )
        self.contributor_type_mapping = self._optional_ref_data_mapping(
            "/contributor-types",
            "contributorTypes",
            contributor_type_map,
            "ContributorTypeMapping",
        )

        # Types resolved by FOLIO name or UUID, as note types are for holdings and items
        self._identifier_types = self._name_to_id("/identifier-types", "identifierTypes")
        self._contributor_name_types = self._name_to_id(
            "/contributor-name-types", "contributorNameTypes"
        )
        self._instance_note_types = self._name_to_id("/instance-note-types", "instanceNoteTypes")
        self._identifier_type_mapping = self._optional_ref_data_mapping(
            "/identifier-types", "identifierTypes", identifier_type_map, "IdentifierTypeMapping"
        )
        self._contributor_name_type_mapping = self._optional_ref_data_mapping(
            "/contributor-name-types",
            "contributorNameTypes",
            contributor_name_type_map,
            "ContributorNameTypeMapping",
        )
        self._instance_note_type_mapping = self._optional_ref_data_mapping(
            "/instance-note-types",
            "instanceNoteTypes",
            instance_note_type_map,
            "InstanceNoteTypeMapping",
        )
        # Fail early if hardcoded values in the mapping file are not valid FOLIO values
        self._validate_hardcoded_note_type_values(self._identifier_types, ".identifierTypeId")
        self._validate_hardcoded_note_type_values(
            self._contributor_name_types, ".contributorNameTypeId"
        )
        self._validate_hardcoded_note_type_values(self._instance_note_types, ".instanceNoteTypeId")

    def _optional_ref_data_mapping(
        self, path: str, array_name: str, the_map, blurb_id: str
    ) -> RefDataMapping | None:
        if not the_map:
            return None
        return RefDataMapping(self.folio_client, path, array_name, the_map, "name", blurb_id)

    def _name_to_id(self, path: str, array_name: str) -> dict:
        return {
            normalize_for_compare(ref["name"]): ref["id"]
            for ref in self.folio_client.folio_get_all(path, array_name, "", 1000)
        }

    def perform_additional_mappings(
        self, legacy_ids: List[str] | str, folio_rec: Dict, file_def: FileDefinition
    ):
        folio_rec["source"] = "FOLIO"
        self.handle_suppression(folio_rec, file_def)
        self.map_statistical_codes(folio_rec, file_def)
        self.map_statistical_code_ids(legacy_ids, folio_rec)
        if getattr(self.task_configuration, "add_administrative_notes_with_legacy_ids", False):
            self.add_legacy_id_to_admin_note(folio_rec, str(legacy_ids))

    def handle_suppression(self, folio_record: Dict, file_def: FileDefinition):
        folio_record["discoverySuppress"] = file_def.discovery_suppressed
        folio_record["staffSuppress"] = file_def.staff_suppressed
        self.migration_report.add(
            "Suppression",
            i18n_t("Suppressed from discovery") + f" = {folio_record['discoverySuppress']}",
        )
        self.migration_report.add(
            "Suppression",
            i18n_t("Staff suppressed") + f" = {folio_record['staffSuppress']}",
        )

    def get_prop(self, legacy_object, folio_prop_name, index_or_id, schema_default_value):  # noqa: C901
        if folio_prop_name == "instanceTypeId":
            return self.get_mapped_ref_data_value(
                self.instance_type_mapping, legacy_object, index_or_id, folio_prop_name
            )
        if folio_prop_name.startswith("natureOfContentTermIds"):
            return self._get_optional_mapped_value(
                self.nature_of_content_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
            )
        if folio_prop_name.startswith("instanceFormatIds"):
            return self._get_optional_mapped_value(
                self.instance_format_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
            )
        if folio_prop_name == "modeOfIssuanceId":
            return self._get_optional_mapped_value(
                self.mode_of_issuance_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
            )
        if folio_prop_name.endswith(".contributorTypeId"):
            return self._get_optional_mapped_value(
                self.contributor_type_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
            )
        if folio_prop_name.endswith(".identifierTypeId"):
            return self._resolve_type_prop(
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
                self._identifier_types,
                self._identifier_type_mapping,
                "legacy_identifier_type",
            )
        if folio_prop_name.endswith(".contributorNameTypeId"):
            return self._resolve_type_prop(
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
                self._contributor_name_types,
                self._contributor_name_type_mapping,
                "legacy_contributor_name_type",
            )
        if folio_prop_name.endswith(".instanceNoteTypeId"):
            return self._resolve_type_prop(
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
                self._instance_note_types,
                self._instance_note_type_mapping,
                "legacy_note_type",
            )

        mapped_value = super().get_prop(
            legacy_object, folio_prop_name, index_or_id, schema_default_value
        )
        if mapped_value:
            return mapped_value
        self.migration_report.add("UnmappedProperties", f"{folio_prop_name}")
        return ""

    def _get_optional_mapped_value(
        self,
        ref_data_mapping: RefDataMapping | None,
        legacy_object,
        folio_prop_name,
        index_or_id,
        schema_default_value,
    ):
        """Resolve a value via an optional ref data map, or pass it through as a UUID.

        Without a map file the mapped value is used as is, so hardcoded UUIDs in the
        mapping file keep working.
        """
        if not ref_data_mapping:
            return super().get_prop(
                legacy_object, folio_prop_name, index_or_id, schema_default_value
            )
        return self.get_mapped_ref_data_value(
            ref_data_mapping, legacy_object, index_or_id, folio_prop_name, True
        )

    def _resolve_type_prop(
        self,
        legacy_object,
        folio_prop_name,
        index_or_id,
        schema_default_value,
        types_by_name: dict,
        type_mapping: RefDataMapping | None = None,
        legacy_key: str = "legacy_note_type",
    ):
        raw = super().get_prop(legacy_object, folio_prop_name, index_or_id, schema_default_value)
        raw = str(raw) if raw else ""
        return self._resolve_note_type_id(raw, types_by_name, folio_prop_name, index_or_id)
