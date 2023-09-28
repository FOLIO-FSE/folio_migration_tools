import csv
import json
import logging
import sys
import time
import traceback
import i18n
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.manual_fee_fines_mapper import (
    ManualFeeFinesMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class ManualFeeFinesTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        feefines_map: str
        migration_task_type: str
        files: List[FileDefinition]
        feefines_owner_map: Optional[str]
        feefines_type_map: Optional[str]
        service_point_map: Optional[str]

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.fees_fines

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
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_configuration.files
        )
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
            service_point_map=self.load_ref_data_mapping_file(
                "feefineaction.createdAt",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.service_point_map,
                self.folio_keys,
                False,
            ),
            ignore_legacy_identifier=True,
        )

    def do_work(self):
        logging.info("Getting started!")
        for file in self.task_configuration.files:
            logging.info("Processing %s", file)
            try:
                self.process_single_file(file)
            except Exception as ee:
                error_str = (
                    f"Processing of {file} failed:\n{ee}."
                    "Check source files for empty rows or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add("FailedFiles", f"{file} - {ee}")
                sys.exit()

    def process_single_file(self, file_def: FileDefinition):
        full_path = self.folder_structure.legacy_records_folder / file_def.file_name
        with open(full_path, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                i18n.t("Number of files processed")
            )
            start = time.time()

            for idx, record in enumerate(self.mapper.get_objects(records_file, full_path)):
                try:
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))

                    self.mapper.report_legacy_mapping_no_schema(record)

                    composite_feefine, legacy_id = self.mapper.do_map(
                        record, f"Row {idx + 1}", FOLIONamespaces.fees_fines
                    )

                    self.mapper.perform_additional_mapping(legacy_id, composite_feefine, record)

                    self.mapper.report_folio_mapping(
                        composite_feefine, self.mapper.composite_feefine_schema
                    )

                    self.mapper.store_objects(composite_feefine)

                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(composite_feefine, indent=4))

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as data_error:
                    self.mapper.handle_transformation_record_failed_error(idx, data_error)
                except TransformationFieldMappingError as mapping_error:
                    self.mapper.handle_transformation_field_mapping_error(idx, mapping_error)

                except AttributeError as attribute_error:
                    traceback.print_exc()
                    logging.fatal(attribute_error)
                    logging.info("Quitting...")
                    sys.exit(1)
                except Exception as exception:
                    self.mapper.handle_generic_exception(idx, exception)

                self.print_progress(idx, start)

    def wrap_up(self):
        logging.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            self.mapper.migration_report.write_migration_report(
                i18n.t("Manual fee/fine transformation report"),
                migration_report_file,
                self.start_datetime,
            )

            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )

        self.clean_out_empty_logs()
