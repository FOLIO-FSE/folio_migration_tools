import json
import logging
import sys
from datetime import datetime
from typing import Dict, List
from uuid import uuid4
import requests

from folioclient import FolioClient
from folio_uuid.folio_uuid import FOLIONamespaces
from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.helper import Helper
from migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
from migration_tools.report_blurbs import Blurbs

class OrganizationMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        organization_map: dict,
        # location_map,
        # call_number_type_map,
        # instance_id_map,
        library_configuration: LibraryConfiguration,
    ):

        #Get organization schema
        organization_schema = OrganizationMapper.get_latest_acq_schema_from_github("mod-orgs", "organization")



        super().__init__(
            folio_client,
            organization_schema,
            organization_map,
            None,
            FOLIONamespaces.holdings,
            library_configuration,
        )
    
    def get_prop(self, legacy_organization, folio_prop_name, index_or_id):
        if not self.use_map:
            return legacy_organization[folio_prop_name]

        legacy_organization_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])

        # IF there is a value mapped, return that one
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

        #TODO Add a bunch of special cases like the below for special organization things
        # if folio_prop_name == "permanentLocationId":
        #     return self.get_location_id(legacy_organization, index_or_id, folio_prop_name)

        if any(legacy_organization_keys):
            return legacy_value
        else:
            # edge case
            return ""

    @staticmethod        
    def get_latest_acq_schemas_from_github(owner, repo, module, object):
        '''
        Returns an extended acquisition object based on the schemas in the
        shared submodule "acq-models". The schemas used to build the extended
        object schema are associated with the latest release of the module.
        '''
        
        github_path = "https://raw.githubusercontent.com"

        submodules = OrganizationMapper.get_submodules_of_latest_release(owner, repo)

        # Get the sha's of sunmodules acq-models and raml_utils
        acq_models_sha = next((item for item in submodules 
        if item["mode"] == "160000" and item["path"] == "acq-modules"))

        # # TODO Maybe - fetch raml_utils schemas if deemed necessary
        # raml_utils_sha = next((item for item in submodules 
        # if item["mode"] == "160000" and item["path"] == "raml-utils"))

        acq_models_path = f"{github_path}/{owner}/acq-models/{acq_models_sha}/{module}/schemas"

        req = requests.get(f"{acq_models_path}/{object}.json")
        object_schema = json.loads(req.text)

        # Fetch referenced schemas
        extended_object_schema = OrganizationMapper.build_extended_object(object_schema)

        return extended_object_schema


    @staticmethod
    def get_submodules_of_latest_release(owner, repo):
        github_path = "https://api.github.com/repos/"

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
        ramls_sha = next((item for item in release_tree if item["path"] == "ramls"))

        # Get the tree for the ramls folder
        ramls_path = f"{github_path}/{owner}/{repo}/git/trees/{ramls_sha}"
        req = requests.get(ramls_path)
        req.raise_for_status()
        ramls_tree = json.loads(req.text)

        # Loop through the tree to get the sha of submodules
        submodules = next((item for item in ramls_tree if item["mode"] == "160000"))

        return submodules


    @staticmethod
    def build_extended_object(object_schema, acq_models_path):
        for property_name_level1, property_level1 in object_schema["properties"].items():
            try:
                if property_level1.get("type") == "object" and property_level1.get("$ref"):
                    if "raml-util/schemas/" not in property_level1["$ref"]:
                        logging.info("Fecthing referenced schema for object %s", property_name_level1)
                        ref_object = property_level1["$ref"]
                        schema_url = f"{acq_models_path}/{ref_object}"
                        ref_schema = requests.get(schema_url)
                        property_level1 = dict(
                            property_level1, 
                            **json.loads(ref_schema.text))
                    else:
                        logging.error("Special property not yet implemented: %s", property_name_level1)

                elif property_level1.get("type") == "array" and property_level1.get("items").get("$ref"):
                    # TODO Consider implementing as extra data.
                    if "common/schemas/uuid.json" not in property_level1["items"]["$ref"]:
                        logging.info("Fecthing referenced schema for array object %s",
                        property_name_level1)
                        ref_object = property_level1["items"]["$ref"]
                        schema_url = f"{acq_models_path}/{ref_object}" 
                        ref_schema = requests.get(schema_url)
                        property_level1["items"] = dict(
                            property_level1["items"], 
                            **json.loads(ref_schema.text))
                    else:
                        logging.error("Linked object not yet implemented: %s", property_name_level1)


            except json.decoder.JSONDecodeError as json_error:
                logging.critical(json_error)
                print(
                    f"\nError parsing {schema_url}."
                )

            except Exception as type_error:
                logging.error("Unable to build schema property for %s: %s", property_name_level1, type_error)
