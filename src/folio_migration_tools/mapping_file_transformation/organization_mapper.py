import json
import logging
import sys
import requests
from requests.exceptions import HTTPError

from folioclient import FolioClient
from folio_uuid.folio_uuid import FOLIONamespaces
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)

from folio_migration_tools.report_blurbs import Blurbs


class OrganizationMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        organization_map: dict,
        library_configuration: LibraryConfiguration,
    ):

        # Get organization schema
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

    def get_prop(self, legacy_organization, folio_prop_name, index_or_id):
        if not self.use_map:
            return legacy_organization[folio_prop_name]

        legacy_organization_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])

        # If there is a value mapped, return that one
        if len(legacy_organization_keys) == 1 and folio_prop_name in self.mapped_from_values:
            value = self.mapped_from_values.get(folio_prop_name, "")
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value

        legacy_values = MappingFileMapperBase.get_legacy_vals(
            legacy_organization, legacy_organization_keys
        )

        legacy_value = " ".join(legacy_values).strip()

        if any(legacy_organization_keys):
            return legacy_value
        else:
            # edge case
            return ""

    @staticmethod
    def get_latest_acq_schemas_from_github(owner, repo, module, object):
        """
        Given a repository owner, a repository, a module name and the name
        of a FOLIO acquisition object, returns a schema for that object that
        also includes the schemas of any other referenced acq objects.
        """
        try:
            github_path = "https://raw.githubusercontent.com"
            submodules = OrganizationMapper.get_submodules_of_latest_release(owner, repo)

            # Get the sha's of sunmodules acq-models and raml_utils
            acq_models_sha = next(
                (item["sha"] for item in submodules if item["path"] == "acq-models")
            )

            # # TODO Maybe - fetch raml_utils schemas if deemed necessary
            # raml_utils_sha = next((item["sha"] for item in submodules
            # if item["path"] == "raml-utils"))

            acq_models_path = f"{github_path}/{owner}/acq-models/{acq_models_sha}/{module}/schemas"

            req = requests.get(f"{acq_models_path}/{object}.json")
            req.raise_for_status()

            object_schema = json.loads(req.text)

            # Fetch referenced schemas
            extended_object_schema = OrganizationMapper.build_extended_object(
                object_schema, acq_models_path
            )

            return extended_object_schema

        except requests.exceptions.HTTPError as http_error:
            logging.critical(f"Halting! \t{http_error}")
            sys.exit(2)

        except json.decoder.JSONDecodeError as json_error:
            logging.critical(json_error)
            sys.exit(2)

    @staticmethod
    def get_submodules_of_latest_release(owner, repo):
        """
        Given a repository owner and a repository, identifies the latest
        release of the repository and returns the submodules associated with
        this release.
        """
        github_path = "https://api.github.com/repos"

        # Get metadata for the latest release
        latest_release_path = f"{github_path}/{owner}/{repo}/releases/latest"
        req = requests.get(f"{latest_release_path}")
        req.raise_for_status()
        latest_release = json.loads(req.text)

        # Get the tag assigned to the latest release
        release_tag = latest_release["tag_name"]
        logging.info(f"Using schemas from latest {repo} release: {release_tag}")

        # Get the tree for the latest release
        tree_path = f"{github_path}/{owner}/{repo}/git/trees/{release_tag}"
        req = requests.get(tree_path)
        req.raise_for_status()
        release_tree = json.loads(req.text)

        # Loop through the tree to get the sha of the folder with path "ramls"
        ramls_sha = next((item["sha"] for item in release_tree["tree"] if item["path"] == "ramls"))

        # Get the tree for the ramls folder
        ramls_path = f"{github_path}/{owner}/{repo}/git/trees/{ramls_sha}"
        req = requests.get(ramls_path)
        req.raise_for_status()
        ramls_tree = json.loads(req.text)

        # Loop through the tree to get the sha of submodules
        submodules = [item for item in ramls_tree["tree"] if item["mode"] == "160000"]

        return submodules

    @staticmethod
    def build_extended_object(object_schema, submodule_path):
        """
        Takes an object schema (for example an organization) and the path to a
        submodule repository and returns the same schema with the full schemas
         of subordinate objects (for example aliases).
        """

        supported_types = ["string", "boolean", "number", "integer", "text", "object", "array"]

        try:

            for property_name_level1, property_level1 in object_schema["properties"].items():

                # Report and discard unhandled properties
                if (
                    property_level1.get("type") not in supported_types
                    or "../" in property_level1.get("$ref", "")
                    or "../" in property_level1.get("items", {}).get("$ref", "")
                ):

                    logging.info(f"Property not yet supported: {property_name_level1}")
                    property_level1["type"] = "Deprecated"

                # Handle object properties
                elif property_level1.get("type") == "object" and property_level1.get("$ref"):

                    logging.info("Fecthing referenced schema for object %s", property_name_level1)

                    ref_object = property_level1["$ref"]
                    schema_url = f"{submodule_path}/{ref_object}"

                    req = requests.get(schema_url)
                    req.raise_for_status()

                    property_level1 = dict(property_level1, **json.loads(req.text))

                # Handle arrays of items properties
                elif property_level1.get("type") == "array" and property_level1.get("items").get(
                    "$ref"
                ):

                    logging.info(
                        "Fetching referenced schema for array object %s", property_name_level1
                    )

                    ref_object = property_level1["items"]["$ref"]
                    schema_url = f"{submodule_path}/{ref_object}"

                    req = requests.get(schema_url)
                    req.raise_for_status()

                    property_level1["items"] = dict(
                        property_level1["items"], **json.loads(req.text)
                    )

            return object_schema

        except HTTPError as he:
            logging.error(he)
