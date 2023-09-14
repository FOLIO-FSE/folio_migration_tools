import json
import logging
import os
import re
import sys
import urllib.parse
import uuid
import i18n

import httpx
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient
from httpx import HTTPError

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)


class CompositeOrderMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        composite_order_map: dict,
        organizations_id_map: dict,
        instance_id_map: dict,
        acquisition_method_map,
        payment_status_map,
        receipt_status_map,
        workflow_status_map,
        location_map,
        funds_map,
        funds_expense_class_map=None,
    ):
        # Get organization schema
        self.composite_order_schema = CompositeOrderMapper.get_latest_acq_schemas_from_github(
            "folio-org", "mod-orders", "mod-orders", "composite_purchase_order"
        )

        super().__init__(
            folio_client,
            self.composite_order_schema,
            composite_order_map,
            None,
            FOLIONamespaces.orders,
            library_configuration,
        )
        logging.info("Loading Instance ID map...")
        self.instance_id_map = instance_id_map
        self.organizations_id_map = organizations_id_map
        self.folio_organization_cache = {}

        self.acquisitions_methods_mapping = RefDataMapping(
            self.folio_client,
            "/orders/acquisition-methods",
            "acquisitionMethods",
            acquisition_method_map,
            "value",
            "AcquisitionMethodMapping",
        )
        logging.info("Init done")
        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            "OrderLineLocationMapping",
        )

        self.folio_client: FolioClient = folio_client
        self.notes_mapper: NotesMapper = NotesMapper(
            library_configuration,
            self.folio_client,
            composite_order_map,
            FOLIONamespaces.note,
            True,
        )
        self.notes_mapper.migration_report = self.migration_report

    def get_prop(self, legacy_order, folio_prop_name: str, index_or_id, schema_default_value):
        if folio_prop_name.endswith(".acquisitionMethod"):
            return self.get_mapped_ref_data_value(
                self.acquisitions_methods_mapping,
                legacy_order,
                folio_prop_name,
                index_or_id,
                False,
            )

        elif re.compile(r"compositePoLines\[(\d+)\]\.id").fullmatch(folio_prop_name):
            return str(uuid.uuid4())

        elif re.compile(r"notes\[\d+\]\.").match(folio_prop_name):
            return ""

        if folio_prop_name.endswith(".locationId"):
            return self.get_mapped_ref_data_value(
                self.location_mapping,
                legacy_order,
                folio_prop_name,
                index_or_id,
                False,
            )

        mapped_value = super().get_prop(
            legacy_order, folio_prop_name, index_or_id, schema_default_value
        )

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

            req = httpx.get(
                f"{acq_models_path}/{object}.json",
                headers=github_headers,
                follow_redirects=True,
                timeout=None,
            )
            req.raise_for_status()

            object_schema = json.loads(req.text)

            return CompositeOrderMapper.build_extended_object(
                object_schema, acq_models_path, github_headers
            )
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
        req = httpx.get(
            f"{latest_release_path}", headers=github_headers, follow_redirects=True, timeout=None
        )
        req.raise_for_status()
        latest_release = json.loads(req.text)

        # Get the tag assigned to the latest release
        release_tag = latest_release["tag_name"]
        logging.info(f"Using schemas from latest {repo} release: {release_tag}")

        # Get the tree for the latest release
        tree_path = f"{github_path}/{owner}/{repo}/git/trees/{release_tag}"
        req = httpx.get(tree_path, headers=github_headers, follow_redirects=True, timeout=None)
        req.raise_for_status()
        release_tree = json.loads(req.text)

        # Loop through the tree to get the sha of the folder with path "ramls"
        ramls_sha = next((item["sha"] for item in release_tree["tree"] if item["path"] == "ramls"))

        # Get the tree for the ramls folder
        ramls_path = f"{github_path}/{owner}/{repo}/git/trees/{ramls_sha}"
        req = httpx.get(ramls_path, headers=github_headers, follow_redirects=True, timeout=None)
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
                    logging.info("Fetching referenced schema for object %s", property_name_level1)
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

            req = httpx.get(
                schema_url, headers=github_headers, follow_redirects=True, timeout=None
            )
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
            req = httpx.get(
                schema_url, headers=github_headers, follow_redirects=True, timeout=None
            )
            req.raise_for_status()
            return dict(property["items"], **json.loads(req.text))
        except Exception as ee:
            logging.error(ee)
            return {}

    def perform_additional_mapping(self, index_or_id, composite_order):
        # Get organization UUID from FOLIO
        composite_order["vendor"] = self.get_folio_organization_uuid(
            index_or_id, composite_order.get("vendor")
        )

        # Replace legacy bib ID with instance UUID from map
        bib_id = composite_order["compositePoLines"][0].pop("instanceId", "")
        if matching_instance := self.get_folio_instance_uuid(index_or_id, bib_id):
            composite_order["compositePoLines"][0]["instanceId"] = matching_instance

        return composite_order

    def get_matching_record_from_folio(
        self,
        index_or_id,
        cache: dict,
        path: str,
        match_property: str,
        match_value: str,
        result_type: str,
    ):
        if match_value in cache:
            return cache[match_value]
        else:
            query = f'?query=({match_property}=="{match_value}")'
            if matching_record := next(
                self.folio_client.folio_get_all(path, result_type, query), None
            ):
                cache[match_value] = matching_record
                return matching_record

    def get_folio_organization_uuid(self, index_or_id, org_code):
        if self.organizations_id_map:
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n.t("Organizations linked using organizations_id_map"),
            )
            if matching_org := self.organizations_id_map.get(org_code):
                return matching_org[1]

        if matching_org := self.get_matching_record_from_folio(
            index_or_id,
            self.folio_organization_cache,
            "/organizations-storage/organizations",
            "code",
            org_code,
            "organizations",
        ):
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n.t("Organizations not in ID map, linked using FOLIO lookup"),
            )
            return matching_org["id"]

        else:
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n.t("RECORD FAILED Organization identifier not in ID map/FOLIO"),
            )
            raise TransformationRecordFailedError(
                index_or_id,
                "No matching Organization for organization identifier",
                org_code,
            )

    def get_folio_instance_uuid(self, index_or_id, bib_id):
        if matching_instance := self.instance_id_map.get(bib_id):
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n.t("Instances linked using instances_id_map"),
            )
            return matching_instance[1]
        else:
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n.t("Bib identifier not in instances_id_map, no instance linked"),
            )
            Helper.log_data_issue(
                index_or_id,
                "No matching instance for bib identifier",
                bib_id,
            )
