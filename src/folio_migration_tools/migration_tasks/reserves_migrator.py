import csv
import json
import logging
import sys
import time
import traceback
import i18n
from typing import Dict
from urllib.error import HTTPError

import httpx
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_dict import InsensitiveDictReader
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration
from folio_migration_tools.transaction_migration.legacy_reserve import LegacyReserve


class ReservesMigrator(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        migration_task_type: str
        course_reserve_file_path: FileDefinition

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.reserve

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
        self.valid_reserves = []
        super().__init__(library_config, task_configuration)
        with open(
            self.folder_structure.legacy_records_folder
            / task_configuration.course_reserve_file_path.file_name,
            "r",
            encoding="utf-8",
        ) as reserves_file:
            self.semi_valid_reserves = list(
                self.load_and_validate_legacy_reserves(
                    InsensitiveDictReader(reserves_file, dialect="tsv")
                )
            )
            logging.info(
                "Loaded and validated %s reserves in file",
                len(self.semi_valid_reserves),
            )

            self.valid_reserves = self.semi_valid_reserves
        self.t0 = time.time()
        self.failed: Dict = {}
        logging.info("Init completed")

    def do_work(self):
        logging.info("Starting")
        for num_reserves, legacy_reserve in enumerate(self.valid_reserves, start=1):
            t0_migration = time.time()
            self.migration_report.add_general_statistics(i18n.t("Processed reserves"))
            try:
                self.post_single_reserve(legacy_reserve)
            except Exception as ee:
                logging.exception(
                    f"Error in row {num_reserves}  Reserve: {json.dumps(legacy_reserve)} {ee}"
                )
            if num_reserves % 50 == 0:
                logging.info(f"{timings(self.t0, t0_migration, num_reserves)} {num_reserves}")

    def post_single_reserve(self, legacy_reserve: LegacyReserve):
        try:
            path = f"/coursereserves/courselistings/{legacy_reserve.course_listing_id}/reserves"
            if self.folio_put_post(
                path, legacy_reserve.to_dict(), "POST", i18n.t("Posted reserves")
            ):
                self.migration_report.add_general_statistics(
                    i18n.t("Successfully posted reserves")
                )
            else:
                self.migration_report.add_general_statistics(i18n.t("Failure to post reserve"))
        except Exception as ee:
            logging.error(ee)

    def wrap_up(self):
        self.extradata_writer.flush()
        for k, v in self.failed.items():
            self.failed_and_not_dupe[k] = [v.to_dict()]
        self.migration_report.set("GeneralStatistics", "Failed loans", len(self.failed))
        self.write_failed_reserves_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                i18n.t("Reserves migration report"), report_file, self.start_datetime
            )
        self.clean_out_empty_logs()

    def write_failed_reserves_to_file(self):
        # POST /coursereserves/courselistings/40a085bd-b44b-42b3-b92f-61894a75e3ce/reserves
        # Match on legacy course number ()

        csv_columns = ["legacy_identifier", "barcode"]
        with open(self.folder_structure.failed_recs_path, "w+") as failed_reserves_file:
            writer = csv.DictWriter(failed_reserves_file, fieldnames=csv_columns, dialect="tsv")
            writer.writeheader()
            for _k, failed_reserve in self.failed.items():
                writer.writerow(failed_reserve[0])

    def check_barcodes(self):
        """Stub for extension.

        Yields:
            _type_: _description_
        """
        item_barcodes = set()
        self.circulation_helper.load_migrated_item_barcodes(
            item_barcodes, self.task_configuration.item_files, self.folder_structure
        )
        for loan in self.semi_valid_legacy_loans:
            has_item_barcode = loan.item_barcode in item_barcodes or not any(item_barcodes)
            if has_item_barcode:
                self.migration_report.add_general_statistics(
                    i18n.t("Reserve verified against migrated item")
                )
                yield loan
            else:
                self.migration_report.add(
                    "DiscardedLoans", i18n.t("Reserve discarded. Could not find migrated barcode")
                )

    def load_and_validate_legacy_reserves(self, reserves_reader):
        num_bad = 0
        logging.info("Validating legacy loans in file...")
        for legacy_reserve_count, legacy_reserve_dict in enumerate(reserves_reader):
            try:
                legacy_reserve = LegacyReserve(
                    legacy_reserve_dict,
                    self.folio_client,
                    legacy_reserve_count,
                )
                if any(legacy_reserve.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics(i18n.t("Discarded reserves"))
                    for error in legacy_reserve.errors:
                        self.migration_report.add("DiscardedReserves", f"{error[0]} - {error[1]}")
                else:
                    yield legacy_reserve
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_reserve_count} legacy reserves with {num_bad} rotten apples"
        )
        if num_bad / legacy_reserve_count > 0.5:
            q = num_bad / legacy_reserve_count
            logging.error("%s percent of reserves failed to validate.", (q * 100))
            self.migration_report.log_me()
            logging.critical("Halting...")
            sys.exit(1)

    def folio_put_post(self, url, data_dict, verb, action_description=""):
        full_url = f"{self.folio_client.okapi_url}{url}"
        try:
            if verb == "PUT":
                resp = httpx.put(
                    full_url,
                    headers=self.folio_client.okapi_headers,
                    json=data_dict,
                )
            elif verb == "POST":
                resp = httpx.post(
                    full_url,
                    headers=self.folio_client.okapi_headers,
                    json=data_dict,
                )
            else:
                raise TransformationProcessError("Bad verb supplied. This is a code issue.")
            if resp.status_code == 422:
                error_message = json.loads(resp.text)["errors"][0]["message"]
                logging.error(error_message)
                self.migration_report.add(
                    "Details",
                    i18n.t(
                        "%{action} error: %{message}",
                        action=action_description,
                        message=error_message,
                    ),
                )
                resp.raise_for_status()
            elif resp.status_code in [201, 204]:
                self.migration_report.add(
                    "Details",
                    i18n.t("Successfully %{action}", action=action_description)
                    + f" ({resp.status_code})",
                )
            else:
                self.migration_report.add(
                    "Details",
                    i18n.t(
                        "%{action} error. http status: %{status}",
                        action=action_description,
                        status=resp.status_code,
                    ),
                )
                logging.error(json.dumps(data_dict))
                resp.raise_for_status()
            return True
        except HTTPError as exception:
            logging.error(f"{resp.status_code}. {verb} FAILED for {url}")
            traceback.print_exc()
            logging.info(exception)
            return False


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
