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
        # TODO: Modify getlatest from github method in helper to get organization_schema for latest mod-organization-storage release
        commit = "2626278b80d82a5e1995f85c37575561264b93e9"
        req = requests.get(f"https://raw.githubusercontent.com/folio-org/acq-models/{commit}/mod-orgs/schemas/organization.json")
        organization_schema = json.loads(req.text)
        # Fetch referenced schemas
        try:
            for property_name_level1, property_level1 in organization_schema["properties"].items():
                if property_level1.get("type") == "object" and property_level1.get("$ref"):
                    logging.info("Fecthing referenced schema for %s", property_name_level1)
                    ref_object = property_level1["$ref"]
                    schema_url = f"https://raw.githubusercontent.com/folio-org/acq-models/{commit}/mod-orgs/schemas/{ref_object}" 
                    ref_schema = requests.get(schema_url)
                    property_level1["properties"] = json.loads(ref_schema.text).get("properties")
                    property_level1["required"] = json.loads(ref_schema.text).get("required", [])

                elif property_level1.get("type") == "array" and property_level1.get("items").get("$ref"):
                    logging.info("Fecthing referenced schema for %s", property_name_level1)
                    ref_object = property_level1["items"]["$ref"]
                    schema_url = f"https://raw.githubusercontent.com/folio-org/acq-models/{commit}/mod-orgs/schemas/{ref_object}" 
                    ref_schema = requests.get(schema_url)cd 
                    property_level1["items"]["properties"] = json.loads(ref_schema.text).get("properties")
                    property_level1["items"]["required"] = json.loads(ref_schema.text).get("required", [])
        except json.decoder.JSONDecodeError as json_error:
            logging.critical(json_error)
            print(
                f"\nError parsing {schema_url}. Halting. "
            )
            
            print(organization_schema)

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
            