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
        req = requests.get("https://raw.githubusercontent.com/folio-org/acq-models/2626278b80d82a5e1995f85c37575561264b93e9/mod-orgs/schemas/organization.json")
        organization_schema = json.loads(req.text)
        # TODO: Modify getlatest from github method in helper to get organization_schema for latest mod-organization-storage release

        super().__init__(
            folio_client,
            organization_schema,
            organization_map,
            None,
            FOLIONamespaces.holdings,
            library_configuration,
        )
    
    def get_prop(self, legacy_organization, folio_prop_name, index_or_id):
        logging.info(folio_prop_name)