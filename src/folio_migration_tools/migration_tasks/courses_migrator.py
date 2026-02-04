"""Course records migration task.

Migrates course information from CSV files to FOLIO Course Reserves module.
Transforms and validates course data including departments and terms.
"""

import csv
import json
import logging
import sys
import time
import traceback
from typing import Optional, Annotated
from pydantic import Field

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
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
        name: Annotated[
            str,
            Field(
                title="Task name",
                description="The name of the task",
            ),
        ]
        composite_course_map_path: Annotated[
            str,
            Field(
                title="Composite course map path",
                description="Path to the composite course map file",
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="Type of migration task",
            ),
        ]
        courses_file: Annotated[
            FileDefinition,
            Field(
                title="Courses file",
                description="File containing course data",
            ),
        ]
        terms_map_path: Annotated[
            str,
            Field(
                title="Terms map path",
                description="Path to the terms map file",
            ),
        ]
        departments_map_path: Annotated[
            str,
            Field(
                title="Departments map path",
                description="Path to the departments map file",
            ),
        ]
        look_up_instructor: Annotated[
            Optional[bool],
            Field(
                title="Look up instructor",
                description=(
                    "Flag to indicate whether to look up instructors. By default is False."
                ),
            ),
        ] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.course

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
    ):
        """Initialize CoursesMigrator for migrating course reserves.

        Args:
            task_configuration (TaskConfiguration): Courses migration configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
        """
        csv.register_dialect("tsv", delimiter="\t")
        self.task_configuration = task_configuration
        super().__init__(library_config, task_configuration, folio_client)
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
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.mapper.migration_report.write_json_report(raw_report_file)
        self.clean_out_empty_logs()


def timings(t0, t0func, num_objects):
    avg = (time.time() - t0) / num_objects
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}s\tElapsed this time: {elapsed_func:.2f}"
    )
