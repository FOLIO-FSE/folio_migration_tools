import csv
import json
import logging
import sys
import time
import traceback
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.manual_fees_fines_mapper import (
    ManualFeesFinesMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class ManualFeesFinesTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        feesfines_map_path: str
        migration_task_type: str
        feesfines_file: FileDefinition
        feesfines_owner_map: Optional[str]
        feesfines_type_map: Optional[str]

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.account

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")

        super().__init__(library_config, task_configuration, use_logging)
        self.object_type_name = self.get_object_type().name
        self.task_configuration = task_configuration
        self.files = self.list_source_files()
        self.total_records = 0

        self.feesfines_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder / self.task_configuration.feesfines_map_path
        )

        self.results_path = self.folder_structure.created_objects_path
        self.failed_files: List[str] = []

        self.mapper = ManualFeesFinesMapper(
            self.folio_client,
            self.library_configuration,
            self.feesfines_map,
            feesfines_owner_map=self.load_ref_data_mapping_file(
                "feesfines_owner",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.feesfines_owner_map,
                self.folio_keys,
                False,
            ),
            feesfines_type_map=self.load_ref_data_mapping_file(
                "feesfines_type",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.feesfines_type_map,
                self.folio_keys,
                False,
            ),
        )

    def do_work(self):
        logging.info("Starting")
        full_path = (
            self.folder_structure.legacy_records_folder
            / self.task_configuration.fessfines_file.file_name
        )
        logging.info("Processing %s", full_path)
        start = time.time()
        with open(full_path, encoding="utf-8-sig") as records_file:
            for idx, record in enumerate(self.mapper.get_objects(records_file, full_path)):
                try:
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))
                        self.mapper.verify_legacy_record(record, idx)

                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.account
                    )
                    self.mapper.perform_additional_mappings((folio_rec, legacy_id))

                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))
                    self.mapper.store_objects((folio_rec, legacy_id))

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
                    f"Number of Legacy items in {full_path}",
                )
                self.mapper.migration_report.add_general_statistics(
                    "Number of legacy items in total"
                )
                self.print_progress(idx, start)

    def wrap_up(self):
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                "Manual fees/fines migration report", report_file, self.mapper.start_datetime
            )
        self.clean_out_empty_logs()


def timings(t0, t0func, num_objects):
    avg = (time.time() - t0) / num_objects
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}s\tElapsed this time: {elapsed_func:.2f}"
    )
