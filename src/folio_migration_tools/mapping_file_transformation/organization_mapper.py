"""Mapper for transforming organization data to FOLIO Organizations format.

Provides the OrganizationMapper class for mapping legacy vendor/organization data
to FOLIO Organization records. Handles embedded interfaces, contacts, categories,
and extradata object creation.
"""

import logging
import os
import re

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.acq_schema_resolver import (
    resolve_acq_schema,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)

logger = logging.getLogger(__name__)


class OrganizationMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        task_config,
        organization_map: dict,
        organization_types_map,
        address_categories_map,
        email_categories_map,
        phone_categories_map,
    ):
        """Initialize OrganizationMapper for organization transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            library_configuration (LibraryConfiguration): Library configuration.
            task_config: Task configuration for organizations migration.
            organization_map (dict): Mapping configuration for organization fields.
            organization_types_map: Mapping of legacy to FOLIO organization types.
            address_categories_map: Mapping of legacy to FOLIO address categories.
            email_categories_map: Mapping of legacy to FOLIO email categories.
            phone_categories_map: Mapping of legacy to FOLIO phone categories.
        """
        # Build composite organization schema
        release_tag = None
        try:
            release_tag = folio_client.get_module_version("mod-organizations-storage")
            logger.info(
                "Using mod-organizations-storage version from FOLIO environment: %s",
                release_tag,
            )
        except Exception:
            logger.info(
                "Could not determine mod-organizations-storage "
                "version, falling back to latest release"
            )

        if os.environ.get("GITHUB_TOKEN"):
            logger.info("Using GITHUB_TOKEN environment variable for GitHub API Access")
        organization_schema = OrganizationMapper.get_organization_schema(release_tag)

        super().__init__(
            folio_client,
            organization_schema,
            organization_map,
            None,
            FOLIONamespaces.organizations,
            library_configuration,
            task_config,
        )
        self.organization_schema = organization_schema
        # Set up reference data maps
        self.set_up_reference_data_mapping(
            organization_types_map,
            address_categories_map,
            email_categories_map,
            phone_categories_map,
        )

        self.folio_client: FolioClient = folio_client
        self.notes_mapper: NotesMapper = NotesMapper(
            library_configuration,
            None,
            self.folio_client,
            organization_map,
            FOLIONamespaces.note,
            True,
        )
        self.notes_mapper.migration_report = self.migration_report

    # Commence the mapping work
    def get_prop(self, legacy_organization, folio_prop_name, index_or_id, schema_default_value):
        value_tuple = (
            legacy_organization,
            index_or_id,
            folio_prop_name,
        )

        # Perfrom reference data mappings
        if folio_prop_name == "organizationTypes":
            return self.get_mapped_ref_data_value(
                self.organization_types_map,
                *value_tuple,
                False,
            )

        elif re.compile(r"addresses\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.address_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile(r"emails\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.email_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile(r"phoneNumbers\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.phone_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile(r"interfaces\[(\d+)\]\.interfaceCredential.interfaceId").fullmatch(
            folio_prop_name
        ):
            return "replace_with_interface_id"

        return super().get_prop(
            legacy_organization, folio_prop_name, index_or_id, schema_default_value
        )

    def set_up_reference_data_mapping(
        self,
        organization_types_map,
        address_categories_map,
        email_categories_map,
        phone_categories_map,
    ):
        """Set up reference data mappings for organization categories and types.

        Args:
        organization_types_map (dict): Mapping for organization types.
        address_categories_map (dict): Mapping for address categories.
        email_categories_map (dict): Mapping for email categories.
        phone_categories_map (dict): Mapping for phone categories.
        """
        categories_shared_args = (
            self.folio_client,
            "/organizations-storage/categories",
            "categories",
        )

        if address_categories_map:
            self.address_categories_map = RefDataMapping(
                *categories_shared_args, address_categories_map, "value", "CategoriesMapping"
            )
        else:
            self.address_categories_map = None

        if email_categories_map:
            self.email_categories_map = RefDataMapping(
                *categories_shared_args, email_categories_map, "value", "CategoriesMapping"
            )
        else:
            self.email_categories_map = None

        if phone_categories_map:
            self.phone_categories_map = RefDataMapping(
                *categories_shared_args, phone_categories_map, "value", "CategoriesMapping"
            )
        else:
            self.phone_categories_map = None

        if organization_types_map:
            self.organization_types_map = RefDataMapping(
                self.folio_client,
                "/organizations-storage/organization-types",
                "organizationTypes",
                organization_types_map,
                "name",
                "OrganizationTypeMapping",
            )
        else:
            self.organization_types_map = None

    @staticmethod
    def get_organization_schema(release_tag=None) -> dict:
        """Resolve the organization schema with contacts and interfaces expanded.

        In the organization schema, ``contacts`` and ``interfaces`` are arrays of UUIDs. For
        mapping, they are replaced by the full contact / interface object schemas, and an
        interface's credential object is nested under ``interfaceCredential``.
        """
        resolve = OrganizationMapper.fetch_additional_schema
        schema = resolve("organization", release_tag)

        schema["properties"]["contacts"] = {
            "description": "Contacts",
            "type": "array",
            "items": resolve("contact", release_tag),
        }

        interface = resolve("interface", release_tag)
        interface["required"] = ["name"]
        credential = resolve("interface_credential", release_tag)
        credential["required"] = ["username", "password", "interfaceId"]
        interface["properties"]["interfaceCredential"] = credential
        schema["properties"]["interfaces"] = {
            "description": "Interfaces",
            "type": "array",
            "items": interface,
        }
        return schema

    @staticmethod
    def fetch_additional_schema(folio_object, release_tag=None):
        return resolve_acq_schema(
            "mod-orgs", folio_object, release_tag, repo="mod-organizations-storage"
        )
