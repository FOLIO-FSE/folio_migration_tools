import csv
import json
import logging
import sys
import time
import i18n
from typing import Optional
from zoneinfo import ZoneInfo

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.circulation_helper import CirculationHelper
from folio_migration_tools.custom_dict import InsensitiveDictReader
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest


class RequestsMigrator(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
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
        try:
            logging.info("Attempting to retrieve tenant timezone configuration...")
            my_path = (
                "/configurations/entries?query=(module==ORG%20and%20configName==localeSettings)"
            )
            self.tenant_timezone_str = json.loads(
                self.folio_client.folio_get_single_object(my_path)["configs"][0]["value"]
            )["timezone"]
            logging.info("Tenant timezone is: %s", self.tenant_timezone_str)
        except Exception:
            logging.info('Tenant locale settings not available. Using "UTC".')
            self.tenant_timezone_str = "UTC"
        self.tenant_timezone = ZoneInfo(self.tenant_timezone_str)
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
        if any(self.task_configuration.item_files) or any(self.task_configuration.patron_files):
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
        patron = self.circulation_helper.get_user_by_barcode(legacy_request.patron_barcode)
        self.migration_report.add_general_statistics(i18n.t("Patron lookups performed"))

        if not patron:
            logging.error(f"No user with barcode {legacy_request.patron_barcode} found in FOLIO")
            Helper.log_data_issue(
                f"{legacy_request.patron_barcode}",
                "No user with barcode.",
                f"{legacy_request.patron_barcode}",
            )
            self.migration_report.add_general_statistics(
                i18n.t("No user with barcode found in FOLIO")
            )
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        legacy_request.patron_id = patron.get("id")

        item = self.circulation_helper.get_item_by_barcode(legacy_request.item_barcode)
        self.migration_report.add_general_statistics(i18n.t("Item lookups performed"))
        if not item:
            logging.error(f"No item with barcode {legacy_request.item_barcode} found in FOLIO")
            self.migration_report.add_general_statistics(
                i18n.t("No item with barcode found in FOLIO")
            )
            Helper.log_data_issue(
                f"{legacy_request.item_barcode}",
                "No item with barcode",
                f"{legacy_request.item_barcode}",
            )
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        holding = self.circulation_helper.get_holding_by_uuid(item.get("holdingsRecordId"))
        self.migration_report.add_general_statistics(i18n.t("Holdings lookups performed"))
        legacy_request.item_id = item.get("id")
        legacy_request.holdings_record_id = item.get("holdingsRecordId")
        legacy_request.instance_id = holding.get("instanceId")
        if item["status"]["name"] in ["Available"]:
            legacy_request.request_type = "Page"
            logging.info(f'Setting request to Page, since the status is {item["status"]["name"]}')
        self.migration_report.add_general_statistics(
            i18n.t("Valid, prepared requests, ready for posting")
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
            try:
                res, legacy_request = self.prepare_legacy_request(legacy_request)
                if res:
                    if self.circulation_helper.create_request(
                        self.folio_client, legacy_request, self.migration_report
                    ):
                        self.migration_report.add_general_statistics(
                            i18n.t("Successfully migrated requests")
                        )
                    else:
                        self.migration_report.add_general_statistics(
                            i18n.t("Unsuccessfully migrated requests")
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
                sys.exit(1)
            if num_requests % 10 == 0:
                logging.info(f"{timings(self.t0, t0_migration, num_requests)} {num_requests}")
        logging.info(f"{timings(self.t0, t0_migration, num_requests)} {num_requests}")

    def wrap_up(self):
        self.extradata_writer.flush()
        self.write_failed_request_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                i18n.t("Requests migration report"), report_file, self.start_datetime
            )
        self.clean_out_empty_logs()

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
                    i18n.t("Requests successfully verified against migrated users and items")
                )
                yield request
            else:
                self.migration_report.add(
                    "DiscardedLoans",
                    i18n.t(
                        "Requests discarded. Had migrated item barcode: %{item_barcode}.\n Had migrated user barcode: %{patron_barcode}",
                        item_barcode=has_item_barcode,
                        patron_barcode=has_patron_barcode,
                    ),
                )
                self.migration_report.add_general_statistics(
                    i18n.t("Requests that failed verification against migrated users and items")
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
            self.migration_report.add_general_statistics(i18n.t("Requests in file"))
            try:
                legacy_request = LegacyRequest(
                    legacy_request_dict,
                    self.tenant_timezone,
                    legacy_reques_count,
                )
                if any(legacy_request.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics(
                        i18n.t("Requests with valueErrors")
                    )
                    for error in legacy_request.errors:
                        self.migration_report.add("DiscardedRequests", f"{error[0]} - {error[1]}")
                        Helper.log_data_issue(
                            legacy_request.item_barcode,
                            f"{error[0]} - {error[1]}",
                            json.dumps(legacy_request.to_source_dict()),
                        )
                else:
                    self.migration_report.add_general_statistics(
                        i18n.t("Requests with valid source data")
                    )
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
            sys.exit(1)


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
