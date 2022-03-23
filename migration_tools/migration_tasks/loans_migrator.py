from ast import Pass
import csv
import json
import logging
import time
import traceback
from attr import has
from pydantic import BaseModel
from migration_tools.helper import Helper
from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.circulation_helper import CirculationHelper
from migration_tools.custom_dict import InsensitiveDictReader
from migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from migration_tools.migration_report import MigrationReport
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

from typing import List, Optional
from migration_tools.report_blurbs import Blurbs

from migration_tools.transaction_migration.legacy_loan import LegacyLoan


class LoansMigrator(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        utc_difference: int
        migration_task_type: str
        open_loans_file: FileDefinition
        fallback_service_point_id: str
        starting_row: Optional[int] = 1
        item_files: Optional[list[FileDefinition]] = []
        patron_files: Optional[list[FileDefinition]] = []

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.loans

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
        self.valid_legacy_loans = []
        super().__init__(library_config, task_configuration)
        self.circulation_helper = CirculationHelper(
            self.folio_client, task_configuration.fallback_service_point_id
        )
        with open(
            self.folder_structure.legacy_records_folder
            / task_configuration.open_loans_file.file_name,
            "r",
            encoding="utf-8",
        ) as loans_file:
            self.semi_valid_legacy_loans = list(
                self.load_and_validate_legacy_loans(
                    InsensitiveDictReader(loans_file, dialect="tsv")
                )
            )
            logging.info(
                "Loaded and validated %s loans in file",
                len(self.semi_valid_legacy_loans),
            )
        self.valid_legacy_loans = list(self.check_barcodes())
        logging.info(
            "Loaded and validated %s loans against barcodes",
            len(self.valid_legacy_loans),
        )
        self.patron_item_combos = set()
        self.t0 = time.time()
        self.num_duplicate_loans = 0
        self.skipped_since_already_added = 0
        self.processed_items = set()
        self.failed = {}
        self.num_legacy_loans_processed = 0
        self.failed_and_not_dupe = {}
        logging.info("Starting row is %s", task_configuration.starting_row)
        logging.info("Init completed")

    def do_work(self):
        return None
        logging.info("Starting")
        if self.task_configuration.starting_row > 1:
            logging.info(f"Skipping {self.starting_row} records")
        for num_loans, legacy_loan in enumerate(
            self.valid_legacy_loans[self.starting_row :], start=1
        ):
            t0_migration = time.time()
            self.migration_report.add_general_statistics("Processed loans")
            try:
                res_checkout = (
                    self.circulation_helper.check_out_by_barcode_override_iris(
                        legacy_loan
                    )
                )
                self.migration_report.add_general_statistics(
                    res_checkout.migration_report_message
                )

                if not res_checkout.was_successful:
                    res_checkout = self.handle_checkout_failure(
                        legacy_loan, res_checkout
                    )
                    if not res_checkout.was_successful:
                        self.migration_report.add_general_statistics(
                            "Loan failed a second time"
                        )
                        logging.info(
                            f"Loan failed a second time. Item barcode {legacy_loan.item_barcode}"
                        )
                        if legacy_loan.item_barcode not in self.failed:
                            self.failed[legacy_loan.item_barcode] = legacy_loan
                        continue
                    else:
                        self.migration_report.add_general_statistics(
                            "Successfully checked out the second time"
                        )
                        logging.info("Successfully checked out the second time")
                else:
                    if not res_checkout.folio_loan:
                        pass
                    else:
                        if legacy_loan.renewal_count > 0:
                            self.update_open_loan(res_checkout.folio_loan, legacy_loan)
                            self.add_stats("Updated renewal count for loan")
                        # set new statuses
                        if legacy_loan.next_item_status == "Declared lost":
                            self.declare_lost(res_checkout.folio_loan)
                        elif legacy_loan.next_item_status == "Claimed returned":
                            self.claim_returned(res_checkout.folio_loan)
                        elif legacy_loan.next_item_status not in [
                            "Available",
                            "",
                            "Checked out",
                        ]:
                            self.set_item_status(legacy_loan)
                if num_loans % 25 == 0:
                    self.print_dict_to_md_table(self.stats)
                    logging.info(
                        f"{timings(self.t0, t0_migration, num_loans)} {num_loans}"
                    )
            except Exception as ee:  # Catch other exceptions than HTTP errors
                logging.exception(
                    f"Error in row {num_loans}  Item barcode: {legacy_loan.item_barcode} "
                    f"Patron barcode: {legacy_loan.patron_barcode} {ee}"
                )

    def wrap_up(self):
        return None

    def check_barcodes(self):
        user_barcodes = set()
        item_barcodes = set()
        self.load_migrated_item_barcodes(item_barcodes)
        self.load_migrated_user_barcodes(user_barcodes)
        for loan in self.semi_valid_legacy_loans:
            has_item_barcode = loan.item_barcode in item_barcodes
            has_patron_barcode = loan.patron_barcode in user_barcodes
            if has_item_barcode and has_patron_barcode:
                self.migration_report.add_general_statistics(
                    "Loans with both user and item migrated"
                )
                yield loan
            if not has_item_barcode:
                Helper.log_data_issue(
                    "Loan without matched item barcode", json.dumps(loan.to_dict())
                )
            if not has_patron_barcode:
                Helper.log_data_issue(
                    "Loan without matched patron barcode", json.dumps(loan.to_dict())
                )
            self.migration_report.add_general_statistics(
                f"Loans discarded. Had item barcode: {has_item_barcode}. "
                f"Had User barcode: {has_patron_barcode}"
            )

    def load_migrated_user_barcodes(self, user_barcodes):
        if any(self.task_configuration.patron_files):
            for filedef in self.task_configuration.patron_files:
                my_path = self.folder_structure.results_folder / filedef.file_name
                with open(my_path) as patron_file:
                    for row in patron_file:
                        rec = json.loads(row)
                        user_barcodes.add(rec.get("barcode", "None"))
            logging.info("Loaded %s barcodes from users", len(user_barcodes))

    def load_migrated_item_barcodes(self, item_barcodes):
        if any(self.task_configuration.item_files):
            for filedef in self.task_configuration.item_files:
                my_path = self.folder_structure.results_folder / filedef.file_name
                with open(my_path) as item_file:
                    for row in item_file:
                        rec = json.loads(row)
                        item_barcodes.add(rec.get("barcode", "None"))
            logging.info("Loaded %s barcodes from items", len(item_barcodes))

    def load_and_validate_legacy_loans(self, loans_reader):
        num_bad = 0
        logging.info("Validating legacy loans in file...")
        for legacy_loan_count, legacy_loan_dict in enumerate(loans_reader):
            try:
                legacy_loan = LegacyLoan(legacy_loan_dict, legacy_loan_count)
                if any(legacy_loan.errors):
                    num_bad += 1
                    for error in legacy_loan.errors:
                        self.add_to_migration_report(error[0], error[1])
                else:
                    yield legacy_loan
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_loan_count} legacy loans with {num_bad} rotten apples"
        )


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
