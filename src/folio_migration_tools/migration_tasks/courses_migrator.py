import csv
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from datetime import timezone

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import BaseModel

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest


class CoursesMigrator(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        composite_course_map_path: str
        migration_task_type: str
        courses_file: FileDefinition
        terms_map_path: str

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.course

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
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
        self.mapper: CoursesMapper = CoursesMapper(
            self.folio_client,
            self.courses_map,
            self.load_ref_data_mapping_file(
                "terms",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.terms_map_path,
                self.folio_keys,
            ),
            self.library_configuration,
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
                    self.mapper.notes_mapper.map_notes(
                        record, 1, folio_rec[0]["course"]["id"], FOLIONamespaces.course
                    )
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

        # POST /coursereserves/courselistings/40a085bd-b44b-42b3-b92f-61894a75e3ce/reserves
        # Match on legacy course number ()
        """ reserve = {
            "courseListingId": "40a085bd-b44b-42b3-b92f-61894a75e3ce",
            "copiedItem": {"barcode": "Actual thesis"},
            "id": "1650db37-8790-4699-bea0-7190ad6384cc",
        } """

    def wrap_up(self):
        self.write_failed_request_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# Requests migration results   \n")
            report_file.write(f"Time Finished: {datetime.isoformat(datetime.now(timezone.utc))}\n")
            self.migration_report.write_migration_report(report_file)

    def write_failed_request_to_file(self):
        csv_columns = [
            "item_barcode",
            "patron_barcode",
            "request_date",
            "request_expiration_date",
            "comment",
            "request_type",
            "pickup_servicepoint_id",
        ]
        with open(self.folder_structure.failed_recs_path, "w+") as failed_requests_file:
            writer = csv.DictWriter(failed_requests_file, fieldnames=csv_columns, dialect="tsv")
            writer.writeheader()
            failed: LegacyRequest
            for failed in self.failed_requests:
                writer.writerow(failed.to_source_dict())

    def check_barcodes(self):
        user_barcodes = set()
        item_barcodes = set()
        self.circulation_helper.load_migrated_item_barcodes(
            item_barcodes, self.task_configuration.item_files, self.folder_structure
        )
        self.circulation_helper.load_migrated_user_barcodes(
            user_barcodes, self.task_configuration.patron_files, self.folder_structure
        )

        request: LegacyRequest
        for request in self.semi_valid_legacy_requests:
            has_item_barcode = request.item_barcode in item_barcodes
            has_patron_barcode = request.patron_barcode in user_barcodes
            if has_item_barcode and has_patron_barcode:
                self.migration_report.add_general_statistics(
                    "Requests verified against migrated user and item"
                )
                yield request
            else:
                self.migration_report.add(
                    Blurbs.DiscardedLoans,
                    f"Requests discarded. Had migrated item barcode: {has_item_barcode}. "
                    f"Had migrated user barcode: {has_patron_barcode}",
                )
            if not has_item_barcode:
                Helper.log_data_issue(
                    "",
                    "Request without matched item barcode",
                    json.dumps(request.to_source_dict()),
                )
            if not has_patron_barcode:
                Helper.log_data_issue(
                    "",
                    "Request without matched patron barcode",
                    json.dumps(request.to_source_dict()),
                )

    def load_and_validate_legacy_requests(self, requests_reader):
        num_bad = 0
        logging.info("Validating legacy requests in file...")
        for legacy_reques_count, legacy_request_dict in enumerate(requests_reader, start=1):
            try:
                legacy_request = LegacyRequest(
                    legacy_request_dict,
                    self.task_configuration.utc_difference,
                    legacy_reques_count,
                )
                if any(legacy_request.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics("Requests with valueErrors")
                    for error in legacy_request.errors:
                        self.migration_report.add(
                            Blurbs.DiscardedRequests, f"{error[0]} - {error[1]}"
                        )
                        Helper.log_data_issue(
                            legacy_request.item_barcode,
                            f"{error[0]} - {error[1]}",
                            json.dumps(legacy_request.to_source_dict()),
                        )
                else:
                    yield legacy_request
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_reques_count} "
            f"legacy requests with {num_bad} rotten apples"
        )
        if num_bad > 0 and (num_bad / legacy_reques_count) > 0.5:
            q = num_bad / legacy_reques_count
            logging.error("%s percent of requests failed to validate.", (q * 100))
            self.migration_report.log_me()
            logging.critical("Halting...")
            sys.exit()


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
