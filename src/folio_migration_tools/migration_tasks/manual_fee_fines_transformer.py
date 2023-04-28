import csv
import json
import logging
import sys
import time
import traceback
from os.path import isfile
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.manual_fee_fines_mapper import (
    ManualFeeFinesMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class ManualFeeFinesTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        feefines_map: str
        migration_task_type: str
        files: List[FileDefinition]
        feefines_owner_map: Optional[str]
        feefines_type_map: Optional[str]

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.feefines

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")

        super().__init__(library_config, task_configuration, use_logging)
        self.object_type_name = self.get_object_type().name
        self.task_configuration = task_configuration
        self.files = self.list_source_files()
        self.total_records = 0

        self.feefines_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder / self.task_configuration.feefines_map
        )

        self.results_path = self.folder_structure.created_objects_path
        self.failed_files: List[str] = []

        self.folio_keys = []
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.feefines_map
        )

        self.mapper = ManualFeeFinesMapper(
            self.folio_client,
            self.library_configuration,
            self.task_configuration,
            self.feefines_map,
            feefines_owner_map=self.load_ref_data_mapping_file(
                "account.ownerId",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.feefines_owner_map,
                self.folio_keys,
                False,
            ),
            feefines_type_map=self.load_ref_data_mapping_file(
                "account.feeFineId",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.feefines_type_map,
                self.folio_keys,
                False,
            ),
            ignore_legacy_identifier=True,
        )

    def list_source_files(self):
        files = [
            self.folder_structure.data_folder / self.object_type_name / f.file_name
            for f in self.task_configuration.files
            if isfile(self.folder_structure.data_folder / self.object_type_name / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_configuration.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in"
                "{self.folder_structure.data_folder} / {self.object_type_name}"
            )
        logging.info("Files to process:")
        for filename in files:
            logging.info("\t%s", filename)
        return files

    def do_work(self):
        logging.info("Getting started!")
        for file in self.files:
            logging.info("Processing %s", file)
            try:
                self.process_single_file(file)
            except Exception as ee:
                error_str = (
                    f"Processing of {file} failed:\n{ee}."
                    "Check source files for empty rows or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add(Blurbs.FailedFiles, f"{file} - {ee}")
                sys.exit()

    def process_single_file(self, filename):
        with open(filename, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics("Number of files processed")
            start = time.time()

            for idx, record in enumerate(self.mapper.get_objects(records_file, filename)):
                try:
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))

                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.feefines
                    )

                    # self.mapper.perform_additional_mapping((folio_rec, legacy_id))

                    self.mapper.report_folio_mapping(
                        folio_rec, self.mapper.composite_feefine_schema
                    )

                    self.mapper.store_objects(folio_rec)

                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as data_error:
                    self.mapper.handle_transformation_record_failed_error(idx, data_error)
                except AttributeError as attribute_error:
                    traceback.print_exc()
                    logging.fatal(attribute_error)
                    logging.info("Quitting...")
                    sys.exit(1)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)

                self.mapper.migration_report.add(
                    Blurbs.GeneralStatistics,
                    "Number of rows in source file",
                )
                self.mapper.migration_report.add_general_statistics("Number of records in total")

                self.print_progress(idx, start)

    def wrap_up(self):
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                "Manual fees/fines migration report", report_file, self.mapper.start_datetime
            )
        self.clean_out_empty_logs()


# def timings(t0, t0func, num_objects):
#     avg = (time.time() - t0) / num_objects
#     elapsed = time.time() - t0
#     elapsed_func = time.time() - t0func
#     return (
#         f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
#         f"Average per object: {avg:.2f}s\tElapsed this time: {elapsed_func:.2f}"
#     )
