import csv
import ctypes
import json
import logging
import sys
import time
import i18n
from os.path import isfile
from typing import List
from typing import Optional

from deepdiff import DeepDiff
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic.main import BaseModel

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.order_mapper import (
    CompositeOrderMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


# Read files and do some work
class OrdersTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        files: List[FileDefinition]
        orders_mapping_file_name: str
        organizations_code_map_file_name: str
        acquisition_method_map_file_name: str
        payment_status_map_file_name: Optional[str] = ""
        receipt_status_map_file_name: Optional[str] = ""
        workflow_status_map_file_name: Optional[str] = ""
        location_map_file_name: Optional[str] = ""
        funds_map_file_name: Optional[str] = ""
        funds_expense_class_map_file_name: Optional[str] = ""

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.orders

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")

        super().__init__(library_config, task_config, use_logging)
        self.object_type_name = self.get_object_type().name
        self.task_config = task_config
        self.files = self.list_source_files()
        self.total_records = 0
        self.current_folio_record: dict = {}
        self.orders_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder / self.task_config.orders_mapping_file_name
        )
        self.results_path = self.folder_structure.created_objects_path
        self.failed_files: List[str] = []

        self.folio_keys = []
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.orders_map
        )
        self.minted_ids: set = set()

        self.mapper = CompositeOrderMapper(
            self.folio_client,
            self.library_configuration,
            self.orders_map,
            self.load_id_map(self.folder_structure.organizations_id_map_path, True),
            self.load_id_map(self.folder_structure.instance_id_map_path, True),
            self.load_ref_data_mapping_file(
                "acquisitionMethod",
                self.folder_structure.mapping_files_folder
                / self.task_config.acquisition_method_map_file_name,
                self.folio_keys,
            ),
            self.load_ref_data_mapping_file(  # Not required, on POL
                "paymentStatus",
                self.folder_structure.mapping_files_folder
                / self.task_config.payment_status_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(  # Not required, on POL
                "receiptStatus",
                self.folder_structure.mapping_files_folder
                / self.task_config.receipt_status_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(  # Not required
                "workflowStatus",
                self.folder_structure.mapping_files_folder
                / self.task_config.workflow_status_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(
                "locationMap",
                self.folder_structure.mapping_files_folder
                / self.task_config.location_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(  # Required if there was is a fund.
                "fundsMap",
                self.folder_structure.mapping_files_folder / self.task_config.funds_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(  # Todo: The property in the schema has no type
                "fundsExpenseClassMap",
                self.folder_structure.mapping_files_folder
                / self.task_config.funds_expense_class_map_file_name,
                self.folio_keys,
                False,
            ),
        )

    def list_source_files(self):
        files = [
            self.folder_structure.data_folder / self.object_type_name / f.file_name
            for f in self.task_config.files
            if isfile(self.folder_structure.data_folder / self.object_type_name / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in"
                "{self.folder_structure.data_folder} / {self.object_type_name}"
            )
        logging.info("Files to process:")
        for filename in files:
            logging.info("\t%s", filename)
        return files

    def process_single_file(self, filename):
        with open(filename, encoding="utf-8-sig") as records_file, open(
            self.folder_structure.created_objects_path, "w+"
        ) as results_file:
            self.mapper.migration_report.add_general_statistics(
                i18n.t("Number of files processed")
            )
            start = time.time()
            records_processed = 0
            for idx, record in enumerate(self.mapper.get_objects(records_file, filename)):
                records_processed += 1

                try:
                    # Print first legacy record, then first transformed record
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))

                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.orders, True
                    )
                    self.mapper.perform_additional_mapping(legacy_id, folio_rec)

                    self.mapper.migration_report.add_general_statistics(
                        i18n.t("TOTAL Purchase Order Lines created")
                    )
                    self.mapper.report_folio_mapping(folio_rec, self.mapper.composite_order_schema)
                    self.mapper.notes_mapper.map_notes(
                        record,
                        legacy_id,
                        folio_rec["id"],
                        FOLIONamespaces.orders,
                    )

                    self.merge_into_orders_with_embedded_pols(folio_rec, results_file)

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)

                # TODO Rewrite to base % value on number of rows in file
                if idx > 1 and idx % 50 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(  # pylint: disable=logging-fstring-interpolation
                        f"{idx:,} records processed. Recs/sec: {elapsed_formatted} "
                    )

            self.total_records = records_processed

            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {filename} containing {self.total_records:,} records. "
                f"Total records processed: {self.total_records:,}"
            )
            logging.info("Storing last record to disk")
            Helper.write_to_file(results_file, self.current_folio_record)
            self.mapper.migration_report.add_general_statistics(
                i18n.t("TOTAL Purchase Orders created")
            )

    def do_work(self):
        logging.info("Getting started!")
        for file in self.files:
            logging.info("Processing %s", file)
            try:
                print(file)
                self.process_single_file(file)
            except Exception as ee:
                error_str = (
                    f"Processing of {file} failed:\n{ee}."
                    "Check source files for empty lines or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add("FailedFiles", f"{file} - {ee}")
                sys.exit()

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            self.mapper.migration_report.write_migration_report(
                i18n.t("Pruchase Orders and Purchase Order Lines Transformation Report"),
                migration_report_file,
                self.start_datetime,
            )

            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        logging.info("All done!")

    def merge_into_orders_with_embedded_pols(self, folio_rec, results_file):
        # Handle merging and storage
        if not self.current_folio_record:
            self.current_folio_record = folio_rec
        if folio_rec["id"] != self.current_folio_record["id"]:
            # Writes record to file
            Helper.write_to_file(results_file, self.current_folio_record)
            self.mapper.migration_report.add_general_statistics(
                i18n.t("TOTAL Purchase Orders created")
            )
            self.current_folio_record = folio_rec

        else:
            # Merge if possible
            diff = DeepDiff(self.current_folio_record, folio_rec)
            if "compositePoLines" in diff.affected_root_keys:
                self.current_folio_record.get("compositePoLines", []).extend(
                    folio_rec.get("compositePoLines", [])
                )
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Rows merged to create Purchase Orders")
                )
            for key in diff.affected_paths:
                self.mapper.migration_report.add("DiffsBetweenOrders", key)
