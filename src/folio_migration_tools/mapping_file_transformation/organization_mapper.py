import json
import logging
import os
import re
import sys

import httpx
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)


class OrganizationMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        organization_map: dict,
        organization_types_map,
        address_categories_map,
        email_categories_map,
        phone_categories_map,
    ):
        # Build composite organization schema
        if os.environ.get("GITHUB_TOKEN"):
            logging.info("Using GITHB_TOKEN environment variable for Gihub API Access")
        organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
            "folio-org", "mod-organizations-storage", "mod-orgs", "organization"
        )

        super().__init__(
            folio_client,
            organization_schema,
            organization_map,
            None,
            FOLIONamespaces.organizations,
            library_configuration,
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

        elif re.compile("addresses\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.address_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile("emails\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.email_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile("phoneNumbers\[(\d+)\]\.categories\[(\d+)\]").fullmatch(folio_prop_name):
            return self.get_mapped_ref_data_value(
                self.phone_categories_map,
                *value_tuple,
                False,
            )

        elif re.compile("interfaces\[(\d+)\]\.interfaceCredential.interfaceId").fullmatch(
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
        """

        Args:
            organization_types_map (_type_): _description_
            address_categories_map (_type_): _description_
            email_categories_map (_type_): _description_
            phone_categories_map (_type_): _description_
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
    def get_latest_acq_schemas_from_github(owner, repo, module, object):
        """
        Given a repository owner, a repository, a module name and the name
        of a FOLIO acquisition object, returns a schema for that object that
        also includes the schemas of any other referenced acq objects.

        Args:
            owner (_type_): _description_
            repo (_type_): _description_
            module (_type_): _description_
            object (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            # Authenticate when calling GitHub, using an API key stored in .env
            github_headers = {
                "content-type": "application/json",
                "User-Agent": "FOLIO Migration Tools (https://github.com/FOLIO-FSE/folio_migration_tools/)",  # noqa:E501,B950
            }

            if os.environ.get("GITHUB_TOKEN"):
                github_headers["authorization"] = f"token {os.environ.get('GITHUB_TOKEN')}"

            # Start talkign to GitHub...
            github_path = "https://raw.githubusercontent.com"
            submodules = OrganizationMapper.get_submodules_of_latest_release(
                owner, repo, github_headers
            )

            # Get the sha's of sunmodules acq-models and raml_utils
            acq_models_sha = next(
                (item["sha"] for item in submodules if item["path"] == "acq-models")
            )

            # # TODO Maybe - fetch raml_utils schemas if deemed necessary
            # raml_utils_sha = next((item["sha"] for item in submodules
            # if item["path"] == "raml-utils"))

            acq_models_path = f"{github_path}/{owner}/acq-models/{acq_models_sha}/{module}/schemas"

            req = httpx.get(f"{acq_models_path}/{object}.json", headers=github_headers)
            req.raise_for_status()

            object_schema = json.loads(req.text)

            # Fetch referenced schemas
            extended_object_schema = OrganizationMapper.build_extended_object(
                object_schema, acq_models_path, github_headers
            )

            return extended_object_schema

        except httpx.HTTPError as http_error:
            logging.critical(f"Halting! \t{http_error}")
            sys.exit(2)

        except json.decoder.JSONDecodeError as json_error:
            logging.critical(json_error)
            sys.exit(2)

    @staticmethod
    def get_submodules_of_latest_release(owner, repo, github_headers):
        """
        Given a repository owner and a repository, identifies the latest
        release of the repository and returns the submodules associated with
        this release.

        Args:
            owner (_type_): _description_
            repo (_type_): _description_
            github_headers (_type_): _description_

        Returns:
            _type_: _description_
        """

        github_path = "https://api.github.com/repos"

        # Get metadata for the latest release
        latest_release_path = f"{github_path}/{owner}/{repo}/releases/latest"
        req = httpx.get(f"{latest_release_path}", headers=github_headers, timeout=None)
        req.raise_for_status()
        latest_release = json.loads(req.text)

        # Get the tag assigned to the latest release
        release_tag = latest_release["tag_name"]
        logging.info(f"Using schemas from latest {repo} release: {release_tag}")

        # Get the tree for the latest release
        tree_path = f"{github_path}/{owner}/{repo}/git/trees/{release_tag}"
        req = httpx.get(tree_path, headers=github_headers, timeout=None)
        req.raise_for_status()
        release_tree = json.loads(req.text)

        # Loop through the tree to get the sha of the folder with path "ramls"
        ramls_sha = next((item["sha"] for item in release_tree["tree"] if item["path"] == "ramls"))

        # Get the tree for the ramls folder
        ramls_path = f"{github_path}/{owner}/{repo}/git/trees/{ramls_sha}"
        req = httpx.get(ramls_path, headers=github_headers, timeout=None)
        req.raise_for_status()
        ramls_tree = json.loads(req.text)

        # Loop through the tree to get the sha of submodules
        submodules = [item for item in ramls_tree["tree"] if item["mode"] == "160000"]

        return submodules

    @staticmethod
    def build_extended_object(object_schema, submodule_path, github_headers):
        """
        Takes an object schema (for example an organization) and the path to a
        submodule repository and returns the same schema with the full schemas
        of subordinate objects (for example aliases).

        Args:
            object_schema (_type_): _description_
            submodule_path (_type_): _description_
            github_headers (_type_): _description_

        Returns:
            _type_: _description_
        """

        supported_types = ["string", "boolean", "number", "integer", "text", "object", "array"]

        try:
            for property_name_level1, property_level1 in object_schema["properties"].items():
                # For now, treat references to UUIDs like strings
                # It's not great practice, but it's the way FOLIO mostly handles it
                if "../../common/schemas/uuid.json" in property_level1.get("$ref", ""):
                    property_level1["type"] = "string"

                # Preliminary implementation of tags
                # https://github.com/folio-org/raml/blob/master/schemas/tags.schema
                elif property_name_level1 == "tags":
                    property_level1["properties"] = {
                        "tagList": {
                            "description": "List of tags",
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    }

                elif property_name_level1 == "contacts":
                    contact_schema = OrganizationMapper.fetch_additional_schema("contact")
                    property_level1["items"] = contact_schema

                elif property_name_level1 == "interfaces":
                    interface_schema = OrganizationMapper.fetch_additional_schema("interface")
                    interface_schema["required"] = ["name"]

                    # Temporarily add the credential object as a subproperty
                    interface_credential_schema = OrganizationMapper.fetch_additional_schema(
                        "interface_credential"
                    )
                    interface_credential_schema["required"] = (
                        ["username", "password", "interfaceId"],
                    )

                    interface_schema["properties"][
                        "interfaceCredential"
                    ] = interface_credential_schema

                    property_level1["items"] = interface_schema

                elif (
                    property_level1.get("type") == "array"
                    and property_level1.get("items").get("$ref")
                    == "../../common/schemas/uuid.json"
                ):
                    property_level1["items"]["type"] = "string"

                # Report and discard unhandled properties
                elif (
                    property_level1.get("type") not in supported_types
                    or "../" in property_level1.get("$ref", "")
                    or "../" in property_level1.get("items", {}).get("$ref", "")
                ):
                    logging.info(f"Property not yet supported: {property_name_level1}")

                # Handle object properties
                elif property_level1.get("type") == "object" and property_level1.get("$ref"):
                    logging.info("Fecthing referenced schema for object %s", property_name_level1)

                    ref_object = property_level1["$ref"]
                    schema_url = f"{submodule_path}/{ref_object}"

                    req = httpx.get(schema_url, headers=github_headers, timeout=None)
                    req.raise_for_status()

                    property_level1 = dict(property_level1, **json.loads(req.text))

                elif property_level1.get("type") == "array" and property_level1.get("items").get(
                    "$ref"
                ):
                    ref_object = property_level1["items"]["$ref"]
                    schema_url = f"{submodule_path}/{ref_object}"

                    req = httpx.get(schema_url, headers=github_headers, timeout=None)
                    req.raise_for_status()

                    property_level1["items"] = dict(
                        property_level1["items"], **json.loads(req.text)
                    )

            return object_schema

        except httpx.HTTPError as he:
            logging.error(he)

    @staticmethod
    def fetch_additional_schema(folio_object):
        additional_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
            "folio-org", "mod-organizations-storage", "mod-orgs", folio_object
        )
        return additional_schema
