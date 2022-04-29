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
    def get_latest_acq_schema_from_github(module, object):
        '''
        Fetches the schema for the main object, for example an organization.
        Loops through the properties in the fetched schema. 
        For every property of type object or array that contains a $ref subproperty,
        fetches the referenced schema and adds the properties from the referenced schema to the main object property.
        '''
        # TODO: Modify getlatest from github method in helper to get organization_schema for latest mod-organization-storage release
        commit = "2626278b80d82a5e1995f85c37575561264b93e9"
        acq_schemas_url = f"https://raw.githubusercontent.com/folio-org/acq-models/{commit}/{module}/schemas/"
        req = requests.get(f"{acq_schemas_url}/{object}.json")
        object_schema = json.loads(req.text)
        
        # Fetch referenced schemas
        try:
            for property_name_level1, property_level1 in object_schema["properties"].items():
                if property_level1.get("type") == "object" and property_level1.get("$ref"):
                    if "raml-util/schemas/" in property_level1["$ref"]:
                        logging.error("Special property not yet implemented: %s", property_name_level1)
                    else:
                        logging.info("Fecthing referenced schema for object %s", property_name_level1)
                        ref_object = property_level1["$ref"]
                        schema_url = f"{acq_schemas_url}/{ref_object}" 
                        ref_schema = requests.get(schema_url)
                        property_level1 = dict(property_level1, **json.loads(ref_schema.text))

                elif property_level1.get("type") == "array" and property_level1.get("items").get("$ref"):
                    # TODO Consider implementing as extra data.
                    if "common/schemas/uuid.json" in property_level1["items"]["$ref"]:
                        logging.error("Linked object not yet implemented: %s", property_name_level1)
                    else:
                        logging.info("Fecthing referenced schema for array object %s", property_name_level1)
                        ref_object = property_level1["items"]["$ref"]
                        schema_url = f"{acq_schemas_url}/{ref_object}" 
                        ref_schema = requests.get(schema_url)
                        property_level1["items"] = dict(property_level1["items"], **json.loads(ref_schema.text))

        except json.decoder.JSONDecodeError as json_error:
            logging.critical(json_error)
            print(
                f"\nError parsing {schema_url}."
            )

        except Exception as type_error:
            logging.error("Unable to build schema property for %s: %s", property_name_level1, type_error)

        return object_schema
