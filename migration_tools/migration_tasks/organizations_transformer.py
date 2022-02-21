# This is a work in progress
import csv
import ctypes
import json
import logging
import sys
import time
import traceback
import uuid
from os.path import isfile
from pathlib import Path
from typing import List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)

from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)

# TODO Create OrganizationMapper 
# TODO from migration_tools.mapping_file_transformation.organization_mapper import OrganizationMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.report_blurbs import Blurbs
from pydantic.main import BaseModel

from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

class OrganizationsTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        files: List[FileDefinition]
        organizations_mapping_file_name: str

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.organizations

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")

        super().__init__(library_config, task_config)
        self.task_config = task_config
        self.files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(self.files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in {self.folder_structure.data_folder}/{self.get_object_type().name}"
            )
        logging.info("Files to process:")
        for filename in self.files:
            logging.info("\t%s", filename.file_name)
        
        self.total_records = 0
        self.folio_keys = []
        self.items_map = self.setup_records_map()
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        self.failed_files: List[str] = list()
    
    def wrap_up(self):
        logging.info("Wrapping up!")


    def do_work(self):
        logging.info("Getting started!")
        for filename in self.files:
            try:
                logging.info("\t%s", filename.file_name)
                self.do_actual_work(filename)
            # Something goes really wrong and we want to stop the script
            except TransformationProcessError as tpe:
                logging.critical(tpe)
                sys.exit()
            except Exception as e:
                print(f"Something unexpected happend! {e}")
                raise e


        # Create organization
        # Create contacts
        # Create credentials

        # TODO Hemläxa: använd json-fil med fungerande mappning
        # TODO Sapa ett megaorganisationsobjekt
        # TODO Skapa schema av objektet med något verktyg!
        # TODO Behöver schemat motsvaras av ett objekt? T.ex.
        
        '''
        "organizationMigrationObject": {
            "organization": {
                "id": "uuid",
                "name": "string",
                "contacts": [uuid, uuid]
            },
            "contacts": [
                {
                "id": "uuid",
                "name": "string",
                "organizationId": "uuid"
                }
            ],
            "credentials": [
                {
                "id": "uuid",
                "username": "string"
                "contactId": "uuid"
                }
            ],
            "notes": [
                {
                "id": "uuid",
                "text": "string"
                "organizationId": "uuid"
                }
            ]
        }
        '''


    def do_actual_work(self, filename):
        for i in range(1,5):
            logging.info(i)
            try:
                if i == 2:
                    raise TransformationRecordFailedError("","I like everyone equally", str(i))
            except TransformationRecordFailedError as trfe:
                trfe.log_it()

        raise TransformationProcessError("","Error reading file with name", filename.file_name)
