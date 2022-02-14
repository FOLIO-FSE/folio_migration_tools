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

from migration_tools.helper import Helper
from migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from pydantic import BaseModel

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
    
    def wrap_up(self):
        logging.info("Wrapping up!")

    def do_work(self):
        logging.info("Getting started!")

        # Create organization
        # Create contacts
        # Create credentials

        # TODO Hemläxa: använd json-fil med fungerande mappning
        # TODO Skapa schema av den med något verktyg!
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