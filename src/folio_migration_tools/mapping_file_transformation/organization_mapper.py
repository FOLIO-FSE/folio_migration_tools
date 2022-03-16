import json
import logging
import sys
from datetime import datetime
from typing import Dict, List
from uuid import uuid4

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
        # TODO: Get latest schema from get latest from github method in folioclient
        self.organization_schema = folio_client.get_latest_from_github(
            "folio-org", "mod-organizations", "ramls/organizations.raml"
        )

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