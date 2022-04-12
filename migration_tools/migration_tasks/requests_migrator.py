import csv
from datetime import datetime
import json
import sys
import logging
import time
from datetime import timezone
from pydantic import BaseModel
from migration_tools.helper import Helper
from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.circulation_helper import CirculationHelper
from migration_tools.custom_dict import InsensitiveDictReader
from migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from migration_tools.migration_report import MigrationReport
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

from typing import Dict, List, Optional
from migration_tools.report_blurbs import Blurbs

from migration_tools.transaction_migration.legacy_request import LegacyRequest


class RequestsMigrator(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        utc_difference: int
        migration_task_type: str
        open_requests_file: FileDefinition
        starting_row: Optional[int] = 1
        item_files: Optional[list[FileDefinition]] = []
        patron_files: Optional[list[FileDefinition]] = []

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.requests

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
        self.valid_legacy_requests = []
        super().__init__(library_config, task_configuration)
        self.circulation_helper = CirculationHelper(
            self.folio_client,
            "",
            self.migration_report,
        )
        with open(
            self.folder_structure.legacy_records_folder
            / task_configuration.open_requests_file.file_name,
            "r",
            encoding="utf-8",
        ) as requests_file:
            self.semi_valid_legacy_requests = list(
                self.load_and_validate_legacy_requests(
                    InsensitiveDictReader(requests_file, dialect="tsv")
                )
            )
            logging.info(
                "Loaded and validated %s requests in file",
                len(self.semi_valid_legacy_requests),
            )
        if any(self.task_configuration.item_files) or any(
            self.task_configuration.patron_files
        ):
            self.valid_legacy_requests = list(self.check_barcodes())
            logging.info(
                "Loaded and validated %s requests against barcodes",
                len(self.valid_legacy_requests),
            )
        else:
            logging.info(
                "No item or user files supplied. Not validating against"
                "previously migrated objects"
            )
            self.valid_legacy_requests = self.semi_valid_legacy_requests
        self.valid_legacy_requests.sort(key=lambda x: x.request_date)
        logging.info("Sorted the list of requests by request date")
        self.t0 = time.time()
        self.skipped_since_already_added = 0
        self.failed_requests = set()
        logging.info("Starting row is %s", task_configuration.starting_row)
        logging.info("Init completed")

    def prepare_legacy_request(self, legacy_request: LegacyRequest):
        patron = self.circulation_helper.get_user_by_barcode(
            legacy_request.patron_barcode
        )
        if not patron:
            logging.error(
                f"No user with barcode {legacy_request.patron_barcode} found in FOLIO"
            )
            Helper.log_data_issue(
                f"{legacy_request.patron_barcode}",
                "No user with barcode",
                f"{legacy_request.patron_barcode}",
            )
            self.migration_report.add_general_statistics("No user with barcode")
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        legacy_request.patron_id = patron.get("id")

        item = self.circulation_helper.get_item_by_barcode(legacy_request.item_barcode)
        if not item:
            logging.error(
                f"No item with barcode {legacy_request.item_barcode} found in FOLIO"
            )
            self.migration_report.add_general_statistics("No item with barcode")
            Helper.log_data_issue(
                f"{legacy_request.item_barcode}",
                "No item with barcode",
                f"{legacy_request.item_barcode}",
            )
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        legacy_request.item_id = item.get("id")
        if item["status"]["name"] in ["Available", "Aged to lost", "Missing"]:
            legacy_request.request_type = "Page"
            logging.info(
                f'Setting request to Page, since the status is {item["status"]["name"]}'
            )
        return True, legacy_request

    def do_work(self):
        logging.info("Starting")
        if self.task_configuration.starting_row > 1:
            logging.info(f"Skipping {(self.task_configuration.starting_row-1)} records")
        for num_requests, legacy_request in enumerate(
            self.valid_legacy_requests[self.task_configuration.starting_row - 1 :],
            start=1,
        ):

            t0_migration = time.time()
            self.migration_report.add_general_statistics("Processed requests")
            try:
                res, legacy_request = self.prepare_legacy_request(legacy_request)
                if res:
                    if self.circulation_helper.create_request(
                        self.folio_client, legacy_request, self.migration_report
                    ):
                        self.migration_report.add_general_statistics(
                            "Successfully processed requests"
                        )
                    else:
                        self.migration_report.add_general_statistics(
                            "Unsuccessfully processed requests"
                        )
                        self.failed_requests.add(legacy_request)
                if num_requests == 1:
                    logging.info(json.dumps(legacy_request.to_dict(), indent=4))
            except Exception:
                logging.exception(
                    "Error in row %s  Item barcode: %s Patron barcode: %s",
                    num_requests,
                    legacy_request.item_barcode,
                    legacy_request.patron_barcode,
                )
                sys.exit()
            if num_requests % 10 == 0:
                logging.info(
                    f"{timings(self.t0, t0_migration, num_requests)} {num_requests}"
                )

    def wrap_up(self):
        self.write_failed_request_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# Requests migration results   \n")
            report_file.write(
                f"Time Finished: {datetime.isoformat(datetime.now(timezone.utc))}\n"
            )
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
            writer = csv.DictWriter(
                failed_requests_file, fieldnames=csv_columns, dialect="tsv"
            )
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
        for legacy_reques_count, legacy_request_dict in enumerate(
            requests_reader, start=1
        ):
            try:
                legacy_request = LegacyRequest(
                    legacy_request_dict,
                    self.task_configuration.utc_difference,
                    legacy_reques_count,
                )
                if any(legacy_request.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics(
                        "Requests with valueErrors"
                    )
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
