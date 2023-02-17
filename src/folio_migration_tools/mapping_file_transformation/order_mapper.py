import json
import logging
import os
import sys
import urllib.parse

import requests
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient
from requests.exceptions import HTTPError

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


class CompositeOrderMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        composite_order_map: dict,
        organizations_id_map: dict,
        acquisition_method_map,
        payment_status_map,
        receipt_status_map,
        workflow_status_map,
        location_map,
        funds_map,
        funds_expense_class_map=None,
    ):

        # Get organization schema
        composite_order_schema = CompositeOrderMapper.get_latest_acq_schemas_from_github(
            "folio-org", "mod-orders", "mod-orders", "composite_purchase_order"
        )

        super().__init__(
            folio_client,
            composite_order_schema,
            composite_order_map,
            None,
            FOLIONamespaces.orders,
            library_configuration,
        )
        self.organizations_id_map: dict = organizations_id_map
        self.acquisitions_methods_mapping = RefDataMapping(
            self.folio_client,
            "/orders/acquisition-methods",
            "acquisitionMethods",
            acquisition_method_map,
            "value",
            Blurbs.AcquisitionMethodMapping,
        )
        logging.info("Init done")

    def get_prop(self, legacy_order, folio_prop_name: str, index_or_id):
        mapped_value = self.get_value_from_map(folio_prop_name, legacy_order, index_or_id)
        if folio_prop_name.endswith(".acquisitionMethod"):
            mapped_val = self.acquisitions_methods_mapping.get_ref_data_mapping(legacy_order)
            return mapped_val["folio_id"]
        else:
            logging.info("No refdata mapping. just returning the value for %s", folio_prop_name)
        return mapped_value

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
                logging.info("Using GITHB_TOKEN environment variable for Gihub API Access")
                github_headers["authorization"] = f"token {os.environ.get('GITHUB_TOKEN')}"

            # Start talkign to GitHub...
            github_path = "https://raw.githubusercontent.com"
            submodules = CompositeOrderMapper.get_submodules_of_latest_release(
                owner, repo, github_headers
            )

            # Get the sha's of submodules acq-models and raml_utils
            acq_models_sha = next(
                (item["sha"] for item in submodules if item["path"] == "acq-models")
            )

            # # TODO Maybe - fetch raml_utils schemas if deemed necessary
            # raml_utils_sha = next((item["sha"] for item in submodules
            # if item["path"] == "raml-utils"))

            acq_models_path = (
                f"{github_path}/{owner}/acq-models/{acq_models_sha}/{module}/schemas/"
            )

            req = requests.get(f"{acq_models_path}/{object}.json", headers=github_headers)
            req.raise_for_status()

            object_schema = json.loads(req.text)

            # Fetch referenced schemas
            extended_object_schema = CompositeOrderMapper.build_extended_object(
                object_schema, acq_models_path, github_headers
            )

            return extended_object_schema

        except requests.exceptions.HTTPError as http_error:
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
        req = requests.get(f"{latest_release_path}", headers=github_headers)
        req.raise_for_status()
        latest_release = json.loads(req.text)

        # Get the tag assigned to the latest release
        release_tag = latest_release["tag_name"]
        logging.info(f"Using schemas from latest {repo} release: {release_tag}")

        # Get the tree for the latest release
        tree_path = f"{github_path}/{owner}/{repo}/git/trees/{release_tag}"
        req = requests.get(tree_path, headers=github_headers)
        req.raise_for_status()
        release_tree = json.loads(req.text)

        # Loop through the tree to get the sha of the folder with path "ramls"
        ramls_sha = next((item["sha"] for item in release_tree["tree"] if item["path"] == "ramls"))

        # Get the tree for the ramls folder
        ramls_path = f"{github_path}/{owner}/{repo}/git/trees/{ramls_sha}"
        req = requests.get(ramls_path, headers=github_headers)
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

        supported_types = [
            "string",
            "boolean",
            "number",
            "integer",
            "text",
            "object",
            "array",
        ]

        try:

            if (
                "properties" not in object_schema
                and "$ref" in object_schema
                and object_schema["type"] == "object"
            ):
                submodule_path["properties"] = CompositeOrderMapper.inject_schema_by_ref(
                    submodule_path, github_headers, object_schema
                ).get("properties")

            for property_name_level1, property_level1 in object_schema.get(
                "properties", {}
            ).items():

                # Report and discard unhandled properties
                if property_level1.get("type") not in supported_types:

                    logging.info(f"Property not yet supported: {property_name_level1}")
                    property_level1["type"] = "Deprecated"

                # Handle object properties
                elif property_level1.get("type") == "object" and property_level1.get("$ref"):

                    logging.info("Fecthing referenced schema for object %s", property_name_level1)
                    actual_path = urllib.parse.urljoin(
                        f"{submodule_path}", object_schema.get("$ref", "")
                    )

                    p1 = CompositeOrderMapper.inject_schema_by_ref(
                        actual_path, github_headers, property_level1
                    )

                    p2 = CompositeOrderMapper.build_extended_object(
                        p1, actual_path, github_headers
                    )
                    object_schema["properties"][property_name_level1] = p2

                # Handle arrays of items properties
                elif property_level1.get("type") == "array" and property_level1.get("items").get(
                    "$ref"
                ):

                    logging.info(
                        "Fetching referenced schema for array object %s", property_name_level1
                    )
                    actual_path = urllib.parse.urljoin(
                        f"{submodule_path}", object_schema.get("$ref", "")
                    )

                    p1 = CompositeOrderMapper.inject_items_schema_by_ref(
                        actual_path, github_headers, property_level1
                    )
                    p2 = CompositeOrderMapper.build_extended_object(
                        p1, actual_path, github_headers
                    )
                    property_level1["items"] = p2
                elif property_level1.get("type") == "string" and property_level1.get("$ref"):
                    logging.info("Fecthing referenced schema for object %s", property_name_level1)
                    actual_path = urllib.parse.urljoin(
                        f"{submodule_path}", object_schema.get("$ref", "")
                    )
                    p1 = CompositeOrderMapper.inject_schema_by_ref(
                        actual_path, github_headers, property_level1
                    )
                    object_schema["properties"][property_name_level1] = p1

            return object_schema

        except HTTPError as he:
            logging.error(he)

    @staticmethod
    def inject_schema_by_ref(submodule_path, github_headers, property: dict):
        base_raml = "https://raw.githubusercontent.com/folio-org/raml/master/"
        try:
            u1 = urllib.parse.urlparse(submodule_path)
            schema_url = urllib.parse.urljoin(u1.geturl(), property["$ref"])
            if schema_url.endswith("tags.schema"):
                schema_url = f"{base_raml}schemas/tags.schema"
            if schema_url.endswith("metadata.schema"):
                schema_url = f"{base_raml}schemas/metadata.schema"

            req = requests.get(schema_url, headers=github_headers)
            req.raise_for_status()
            return dict(property, **json.loads(req.text))
        except Exception as ee:
            logging.error(ee)
            return {}

    @staticmethod
    def inject_items_schema_by_ref(submodule_path, github_headers, property: dict):
        try:
            u1 = urllib.parse.urlparse(submodule_path)
            schema_url = urllib.parse.urljoin(u1.geturl(), property["items"]["$ref"])
            req = requests.get(schema_url, headers=github_headers)
            req.raise_for_status()
            return dict(property["items"], **json.loads(req.text))
        except Exception as ee:
            logging.error(ee)
            return {}
