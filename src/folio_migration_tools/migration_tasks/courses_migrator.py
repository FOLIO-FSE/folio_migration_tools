import csv
import json
import logging
import sys
import time
import traceback
import i18n
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class CoursesMigrator(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        composite_course_map_path: str
        migration_task_type: str
        courses_file: FileDefinition
        terms_map_path: str
        departments_map_path: str
        look_up_instructor: Optional[bool] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.course

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        self.task_configuration = task_configuration
        super().__init__(library_config, task_configuration)
        self.t0 = time.time()
        self.courses_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_configuration.composite_course_map_path
        )
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.courses_map
        )
        terms_map = self.load_ref_data_mapping_file(
            "terms",
            self.folder_structure.mapping_files_folder / self.task_configuration.terms_map_path,
            self.folio_keys,
        )

        departments_map = self.load_ref_data_mapping_file(
            "departments",
            self.folder_structure.mapping_files_folder
            / self.task_configuration.departments_map_path,
            self.folio_keys,
        )
        self.mapper: CoursesMapper = CoursesMapper(
            self.folio_client,
            self.courses_map,
            terms_map,
            departments_map,
            self.library_configuration,
            self.task_configuration,
        )
        logging.info("Init completed")

    def do_work(self):
        logging.info("Starting")
        full_path = (
            self.folder_structure.legacy_records_folder
            / self.task_configuration.courses_file.file_name
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
                        record, f"row {idx}", FOLIONamespaces.course
                    )
                    self.mapper.perform_additional_mappings((folio_rec, legacy_id))
                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))
                    self.mapper.store_objects((folio_rec, legacy_id))
                    self.mapper.notes_mapper.map_notes(
                        record, legacy_id, folio_rec["course"]["id"], FOLIONamespaces.course
                    )

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
                    "GeneralStatistics",
                    i18n.t("Number of Legacy items in %{container}", container=full_path),
                )
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Number of Legacy items in total")
                )
                self.print_progress(idx, start)

    def wrap_up(self):
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("Courses migration report"), report_file, self.mapper.start_datetime
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
