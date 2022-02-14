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

<<<<<<< HEAD:src/folio_migration_tools/migration_tasks/organizations_transformer.py
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)

# TODO Create OrganizationMapper 
from migration_tools.mapping_file_transformation.organization_mapper import OrganizationMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.report_blurbs import Blurbs
from pydantic.main import BaseModel

from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
=======
from migration_tools.helper import Helper
from migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from pydantic import BaseModel
>>>>>>> 921cbcd (create orgs transformer):migration_tools/migration_tasks/organizations_transformer.py

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
<<<<<<< HEAD:src/folio_migration_tools/migration_tasks/organizations_transformer.py
        
        self.total_records = 0
        self.items_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_config.items_mapping_file_name
        )
=======
>>>>>>> 921cbcd (create orgs transformer):migration_tools/migration_tasks/organizations_transformer.py
    
    def wrap_up(self):
        logging.info("Wrapping up!")

    def do_work(self):
        logging.info("Getting started!")
<<<<<<< HEAD:src/folio_migration_tools/migration_tasks/organizations_transformer.py
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

=======
>>>>>>> 921cbcd (create orgs transformer):migration_tools/migration_tasks/organizations_transformer.py

        # Create organization
        # Create contacts
        # Create credentials

        # TODO Hemläxa: använd json-fil med fungerande mappning
<<<<<<< HEAD:src/folio_migration_tools/migration_tasks/organizations_transformer.py
        # TODO Sapa ett megaorganisationsobjekt
        # TODO Skapa schema av objektet med något verktyg!
=======
        # TODO Skapa schema av den med något verktyg!
>>>>>>> 921cbcd (create orgs transformer):migration_tools/migration_tasks/organizations_transformer.py
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
<<<<<<< HEAD:src/folio_migration_tools/migration_tasks/organizations_transformer.py
        '''

    def process_single_file(self, file_name):
        with open(file_name, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                "Number of files processed"
            )
            start = time.time()
            records_processed = 0
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, file_name)
            ):
                records_processed = idx + 1
                try:
                    self.process_holding(idx, record)

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add_general_statistics(
                    "Number of Legacy items in file"
                )
                if idx > 1 and idx % 10000 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(  # pylint: disable=logging-fstring-interpolation
                        f"{idx:,} records processed. Recs/sec: {elapsed_formatted} "
                    )
            self.total_records = records_processed
            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {file_name} containing {self.total_records:,} records. "
                f"Total records processed: {self.total_records:,}"
            )
=======
        '''
>>>>>>> 921cbcd (create orgs transformer):migration_tools/migration_tasks/organizations_transformer.py
