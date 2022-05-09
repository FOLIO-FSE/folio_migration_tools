import copy
import csv
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional
from urllib.error import HTTPError

import requests
from dateutil import parser as du_parser
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import BaseModel

from folio_migration_tools.circulation_helper import CirculationHelper
from folio_migration_tools.custom_dict import InsensitiveDictReader
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.transaction_migration.legacy_loan import LegacyLoan
from folio_migration_tools.transaction_migration.transaction_result import (
    TransactionResult,
)


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
            self.folio_client,
            task_configuration.fallback_service_point_id,
            self.migration_report,
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
        if any(self.task_configuration.item_files) or any(self.task_configuration.patron_files):
            self.valid_legacy_loans = list(self.check_barcodes())
            logging.info(
                "Loaded and validated %s loans against barcodes",
                len(self.valid_legacy_loans),
            )
        else:
            logging.info(
                "No item or user files supplied. Not validating against"
                "previously migrated objects"
            )
            self.valid_legacy_loans = self.semi_valid_legacy_loans
        self.patron_item_combos = set()
        self.t0 = time.time()
        self.num_duplicate_loans = 0
        self.skipped_since_already_added = 0
        self.processed_items = set()
        self.failed = {}
        self.failed_and_not_dupe = {}
        logging.info("Starting row number is %s", task_configuration.starting_row)
        logging.info("Init completed")

    def do_work(self):
        logging.info("Starting")
        starting_index = (
            self.task_configuration.starting_row - 1
            if self.task_configuration.starting_row > 0
            else 0
        )
        if self.task_configuration.starting_row > 1:
            logging.info(f"Skipping {(starting_index)} records")
        for num_loans, legacy_loan in enumerate(self.valid_legacy_loans[starting_index:], start=1):
            t0_migration = time.time()
            self.migration_report.add_general_statistics("Processed loans")
            try:
                self.checkout_single_loan(legacy_loan)
            except Exception as ee:
                logging.exception(
                    f"Error in row {num_loans}  Item barcode: {legacy_loan.item_barcode} "
                    f"Patron barcode: {legacy_loan.patron_barcode} {ee}"
                )
            if num_loans % 25 == 0:
                logging.info(f"{timings(self.t0, t0_migration, num_loans)} {num_loans}")

    def checkout_single_loan(self, legacy_loan: LegacyLoan):
        """Checks a legacy loan out. Retries once if it fails.

        Args:
            legacy_loan (LegacyLoan): The Legacy loan
        """
        res_checkout = self.circulation_helper.check_out_by_barcode(legacy_loan)

        if res_checkout.was_successful:
            self.migration_report.add(Blurbs.Details, "Checked out on first try")
            self.set_renewal_count(legacy_loan, res_checkout)
            self.set_new_status(legacy_loan, res_checkout)
        elif res_checkout.should_be_retried:
            res_checkout2 = self.handle_checkout_failure(legacy_loan, res_checkout)
            if res_checkout2.was_successful and res_checkout2.folio_loan:
                self.migration_report.add(Blurbs.Details, "Checked out on second try")
                logging.info("Checked out on second try")
                self.set_renewal_count(legacy_loan, res_checkout2)
                self.set_new_status(legacy_loan, res_checkout2)
            elif legacy_loan.item_barcode not in self.failed:
                self.failed[legacy_loan.item_barcode] = legacy_loan
                logging.error("Failed on second try: %s", res_checkout2.error_message)
                self.migration_report.add(
                    Blurbs.Details,
                    f"Second failure: {res_checkout2.migration_report_message}",
                )
        elif not res_checkout.should_be_retried:
            logging.error("Failed first time. No retries: %s", res_checkout.error_message)
            self.migration_report.add(
                Blurbs.Details,
                f"Failed 1st time. No retries: {res_checkout.migration_report_message}",
            )

    def set_new_status(self, legacy_loan: LegacyLoan, res_checkout: TransactionResult):
        """Updates checkout loans with their destination statuses

        Args:
            legacy_loan (LegacyLoan): _description_
            res_checkout (TransactionResult): _description_
        """
        # set new statuses
        if legacy_loan.next_item_status == "Declared lost":
            self.declare_lost(res_checkout.folio_loan)
        elif legacy_loan.next_item_status == "Claimed returned":
            self.claim_returned(res_checkout.folio_loan)
        elif legacy_loan.next_item_status not in ["Available", "", "Checked out"]:
            self.set_item_status(legacy_loan)

    def set_renewal_count(self, legacy_loan: LegacyLoan, res_checkout: TransactionResult):
        if legacy_loan.renewal_count > 0:
            self.update_open_loan(res_checkout.folio_loan, legacy_loan)
            self.migration_report.add_general_statistics("Updated renewal count for loan")

    def wrap_up(self):
        for k, v in self.failed.items():
            self.failed_and_not_dupe[k] = [v.to_dict()]
        self.migration_report.set(
            Blurbs.GeneralStatistics, "Failed loans", len(self.failed_and_not_dupe)
        )

        self.write_failed_loans_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# Loans migration results   \n")
            report_file.write(f"Time Finished: {datetime.isoformat(datetime.now(timezone.utc))}\n")
            self.migration_report.write_migration_report(report_file)

    def write_failed_loans_to_file(self):
        csv_columns = [
            "due_date",
            "item_barcode",
            "next_item_status",
            "out_date",
            "patron_barcode",
            "renewal_count",
        ]
        with open(self.folder_structure.failed_recs_path, "w+") as failed_loans_file:
            writer = csv.DictWriter(failed_loans_file, fieldnames=csv_columns, dialect="tsv")
            writer.writeheader()
            for _k, failed_loan in self.failed_and_not_dupe.items():
                writer.writerow(failed_loan[0])

    def check_barcodes(self):
        user_barcodes = set()
        item_barcodes = set()
        self.circulation_helper.load_migrated_item_barcodes(
            item_barcodes, self.task_configuration.item_files, self.folder_structure
        )
        self.circulation_helper.load_migrated_user_barcodes(
            user_barcodes, self.task_configuration.patron_files, self.folder_structure
        )
        for loan in self.semi_valid_legacy_loans:
            has_item_barcode = loan.item_barcode in item_barcodes
            has_patron_barcode = loan.patron_barcode in user_barcodes
            if has_item_barcode and has_patron_barcode:
                self.migration_report.add_general_statistics(
                    "Loans verified against migrated user and item"
                )
                yield loan
            else:
                self.migration_report.add(
                    Blurbs.DiscardedLoans,
                    f"Loans discarded. Had migrated item barcode: {has_item_barcode}. "
                    f"Had migrated user barcode: {has_patron_barcode}",
                )
            if not has_item_barcode:
                Helper.log_data_issue(
                    "", "Loan without matched item barcode", json.dumps(loan.to_dict())
                )
            if not has_patron_barcode:
                Helper.log_data_issue(
                    "",
                    "Loan without matched patron barcode",
                    json.dumps(loan.to_dict()),
                )

    def load_and_validate_legacy_loans(self, loans_reader):
        num_bad = 0
        logging.info("Validating legacy loans in file...")
        for legacy_loan_count, legacy_loan_dict in enumerate(loans_reader):
            try:
                legacy_loan = LegacyLoan(
                    legacy_loan_dict,
                    self.task_configuration.utc_difference,
                    legacy_loan_count,
                )
                if any(legacy_loan.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics("Discarded Loans")
                    for error in legacy_loan.errors:
                        self.migration_report.add(
                            Blurbs.DiscardedLoans, f"{error[0]} - {error[1]}"
                        )
                else:
                    yield legacy_loan
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_loan_count} legacy loans with {num_bad} rotten apples"
        )
        if num_bad / legacy_loan_count > 0.5:
            q = num_bad / legacy_loan_count
            logging.error("%s percent of loans failed to validate.", (q * 100))
            self.migration_report.log_me()
            logging.critical("Halting...")
            sys.exit(1)

    def handle_checkout_failure(
        self, legacy_loan, folio_checkout: TransactionResult
    ) -> TransactionResult:
        """Determines what can be done about a previously failed transaction

        Args:
            legacy_loan (_type_): The legacy loan
            folio_checkout (TransactionResult): The results from the prevous transaction

        Returns:
            TransactionResult: A modified TransactionResult based on the result from the
             handling
        """
        folio_checkout.should_be_retried = False
        if folio_checkout.error_message == "5XX":
            return folio_checkout
        if folio_checkout.error_message.startswith(
            "No patron with barcode"
        ) or folio_checkout.error_message.startswith("Patron barcode already detected"):
            return folio_checkout
        elif folio_checkout.error_message.startswith("No item with barcode"):
            return folio_checkout
        elif folio_checkout.error_message.startswith(
            "Cannot check out item that already has an open loan"
        ):
            return folio_checkout
        elif folio_checkout.error_message.startswith("Aged to lost for item"):
            return self.handle_aged_to_lost_item(legacy_loan)
        elif folio_checkout.error_message == "Declared lost":
            return folio_checkout
        elif folio_checkout.error_message.startswith("Cannot check out to inactive user"):
            return self.checkout_to_inactice_user(legacy_loan)
        else:
            self.migration_report.add(
                Blurbs.Details,
                f"Other checkout failure: {folio_checkout.error_message}",
            )
            # First failure. Add to list of failed loans
            if legacy_loan.item_barcode not in self.failed:
                self.failed[legacy_loan.item_barcode] = legacy_loan
            else:
                logging.debug(
                    f"Loan already in failed. item barcode {legacy_loan.item_barcode} "
                    f"Patron barcode: {legacy_loan.patron_barcode}"
                )
                self.failed_and_not_dupe[legacy_loan.item_barcode] = [
                    legacy_loan,
                    self.failed[legacy_loan.item_barcode],
                ]
                logging.info(
                    f"Duplicate loans (or failed twice) Item barcode: "
                    f"{legacy_loan.item_barcode} Patron barcode: {legacy_loan.patron_barcode}"
                )
                self.migration_report.add(Blurbs.Details, "Duplicate loans (or failed twice)")
                del self.failed[legacy_loan.item_barcode]
            return TransactionResult(False, False, "", "", "")

    def checkout_to_inactice_user(self, legacy_loan) -> TransactionResult:
        logging.info("Cannot check out to inactive user. Activating and trying again")
        user = self.get_user_by_barcode(legacy_loan.patron_barcode)
        expiration_date = user.get("expirationDate", datetime.isoformat(datetime.now()))
        user["expirationDate"] = datetime.isoformat(datetime.now() + timedelta(days=1))
        self.activate_user(user)
        logging.debug("Successfully Activated user")
        res = self.circulation_helper.check_out_by_barcode(legacy_loan)  # checkout_and_update
        self.migration_report.add(Blurbs.Details, res.migration_report_message)
        self.deactivate_user(user, expiration_date)
        logging.debug("Successfully Deactivated user again")
        self.migration_report.add(Blurbs.Details, "Handled inactive users")
        return res

    def handle_aged_to_lost_item(self, legacy_loan) -> TransactionResult:
        logging.debug("Setting Available")
        legacy_loan.next_item_status = "Available"
        self.set_item_status(legacy_loan)
        res_checkout = self.circulation_helper.check_out_by_barcode(legacy_loan)
        legacy_loan.next_item_status = "Aged to lost"
        self.set_item_status(legacy_loan)
        s = "Successfully Checked out Aged to lost item and put the status back"
        logging.info(s)
        self.migration_report.add(Blurbs.Details, s)
        return res_checkout

    def update_open_loan(self, folio_loan: dict, legacy_loan: LegacyLoan):
        due_date = du_parser.isoparse(str(legacy_loan.due_date))
        out_date = du_parser.isoparse(str(legacy_loan.out_date))
        renewal_count = legacy_loan.renewal_count
        # TODO: add logging instead of print out
        try:
            loan_to_put = copy.deepcopy(folio_loan)
            del loan_to_put["metadata"]
            loan_to_put["dueDate"] = due_date.isoformat()
            loan_to_put["loanDate"] = out_date.isoformat()
            loan_to_put["renewalCount"] = renewal_count
            url = f"{self.folio_client.okapi_url}/circulation/loans/{loan_to_put['id']}"
            req = requests.put(
                url,
                headers=self.folio_client.okapi_headers,
                data=json.dumps(loan_to_put),
            )
            if req.status_code == 422:
                error_message = json.loads(req.text)["errors"][0]["message"]
                s = f"Update open loan error: {error_message} {req.status_code}"
                self.migration_report.add(Blurbs.Details, s)
                logging.error(s)
                return False
            elif req.status_code in [201, 204]:
                self.migration_report.add(
                    Blurbs.Details,
                    f"Successfully updated open loan ({req.status_code})",
                )
                return True
            else:
                self.migration_report.add(
                    Blurbs.Details,
                    f"Update open loan error http status: {req.status_code}",
                )
                req.raise_for_status()
            logging.debug("Updating open loan was successful")
            return True
        except HTTPError as exception:
            logging.error(
                f"{req.status_code} PUT FAILED Extend loan to {loan_to_put['dueDate']}"
                f"\t {url}\t{json.dumps(loan_to_put)}"
            )
            traceback.print_exc()
            logging.error(exception)
            return False

    def handle_previously_failed_loans(self, loan):
        if loan["item_id"] in self.failed:
            s = "Loan succeeded but failed previously. Removing from failed    "
            logging.info(s)
            del self.failed[loan["item_id"]]

    def declare_lost(self, folio_loan):
        declare_lost_url = f"/circulation/loans/{folio_loan['id']}/declare-item-lost"
        logging.debug(f"Declare lost url:{declare_lost_url}")
        due_date = du_parser.isoparse(folio_loan["dueDate"])
        data = {
            "declaredLostDateTime": datetime.isoformat(due_date + timedelta(days=1)),
            "comment": "Created at migration. Date is due date + 1 day",
            "servicePointId": str(self.task_configuration.fallback_service_point_id),
        }
        logging.debug(f"Declare lost data: {json.dumps(data, indent=4)}")
        if self.folio_put_post(declare_lost_url, data, "POST", "Declare item as lost"):
            self.migration_report.add(Blurbs.Details, "Successfully declared loan as lost")
        else:
            logging.error(f"Unsuccessfully declared loan {folio_loan} as lost")
            self.migration_report.add(Blurbs.Details, "Unsuccessfully declared loan as lost")
        # TODO: Exception handling

    def claim_returned(self, folio_loan):
        claim_returned_url = f"/circulation/loans/{folio_loan['id']}/claim-item-returned"
        logging.debug(f"Claim returned url:{claim_returned_url}")
        due_date = du_parser.isoparse(folio_loan["dueDate"])
        data = {
            "itemClaimedReturnedDateTime": datetime.isoformat(due_date + timedelta(days=1)),
            "comment": "Created at migration. Date is due date + 1 day",
        }
        logging.debug(f"Claim returned data:\t{json.dumps(data)}")
        if self.folio_put_post(claim_returned_url, data, "POST", "Declare item as lost"):
            self.migration_report.add(
                Blurbs.Details, "Successfully declared loan as Claimed returned"
            )
        else:
            logging.error(f"Unsuccessfully declared loan {folio_loan} as Claimed returned")
            self.migration_report.add(
                Blurbs.Details,
                f"Unsuccessfully declared loan {folio_loan} as Claimed returned",
            )
        # TODO: Exception handling

    def set_item_status(self, legacy_loan: LegacyLoan):
        try:
            # Get Item by barcode, update status.
            item_path = f'item-storage/items?query=(barcode=="{legacy_loan.item_barcode}")'
            item_url = f"{self.folio_client.okapi_url}/{item_path}"
            resp = requests.get(item_url, headers=self.folio_client.okapi_headers)
            resp.raise_for_status()
            data = resp.json()
            folio_item = data["items"][0]
            folio_item["status"]["name"] = legacy_loan.next_item_status
            if self.update_item(folio_item):
                self.migration_report.add(
                    Blurbs.Details,
                    f"Successfully set item status to {legacy_loan.next_item_status}",
                )
                logging.debug(
                    f"Successfully set item with barcode "
                    f"{legacy_loan.item_barcode} to {legacy_loan.next_item_status}"
                )
            else:
                if legacy_loan.item_barcode not in self.failed:
                    self.failed[legacy_loan.item_barcode] = legacy_loan
                logging.error(
                    f"Error when setting item with barcode "
                    f"{legacy_loan.item_barcode} to {legacy_loan.next_item_status}"
                )
                self.migration_report.add(
                    Blurbs.Details,
                    f"Error setting item status to {legacy_loan.next_item_status}",
                )
        except Exception as ee:
            logging.error(
                f"{resp.status_code} when trying to set item with barcode "
                f"{legacy_loan.item_barcode} to {legacy_loan.next_item_status} {ee}"
            )
            raise ee

    def activate_user(self, user):
        user["active"] = True
        self.update_user(user)
        self.migration_report.add(Blurbs.Details, "Successfully activated user")

    def deactivate_user(self, user, expiration_date):
        user["expirationDate"] = expiration_date
        user["active"] = False
        self.update_user(user)
        self.migration_report.add(Blurbs.Details, "Successfully deactivated user")

    def update_item(self, item):
        url = f'/item-storage/items/{item["id"]}'
        return self.folio_put_post(url, item, "PUT", "Update item")

    def update_user(self, user):
        url = f'/users/{user["id"]}'
        self.folio_put_post(url, user, "PUT", "Update user")

    def get_user_by_barcode(self, barcode):
        url = f'{self.folio_client.okapi_url}/users?query=(barcode=="{barcode}")'
        resp = requests.get(url, headers=self.folio_client.okapi_headers)
        resp.raise_for_status()
        data = resp.json()
        return data["users"][0]

    def folio_put_post(self, url, data_dict, verb, action_description=""):
        full_url = f"{self.folio_client.okapi_url}{url}"
        try:
            if verb == "PUT":
                resp = requests.put(
                    full_url,
                    headers=self.folio_client.okapi_headers,
                    data=json.dumps(data_dict),
                )
            elif verb == "POST":
                resp = requests.post(
                    full_url,
                    headers=self.folio_client.okapi_headers,
                    data=json.dumps(data_dict),
                )
            else:
                raise Exception("Bad verb")
            if resp.status_code == 422:
                error_message = json.loads(resp.text)["errors"][0]["message"]
                logging.error(error_message)
                self.migration_report.add(
                    Blurbs.Details, f"{action_description} error: {error_message}"
                )
                resp.raise_for_status()
            elif resp.status_code in [201, 204]:
                self.migration_report.add(
                    Blurbs.Details,
                    f"Successfully {action_description} ({resp.status_code})",
                )
            else:
                self.migration_report.add(
                    Blurbs.Details,
                    f"{action_description} error. http status: {resp.status_code}",
                )

                resp.raise_for_status()
            return True
        except HTTPError as exception:
            logging.error(f"{resp.status_code}. {verb} FAILED for {url}")
            traceback.print_exc()
            logging.info(exception)
            return False

    def change_due_date(self, folio_loan, legacy_loan):
        try:
            api_path = f"{folio_loan['id']}/change-due-date"
            api_url = f"{self.folio_client.okapi_url}/circulation/loans/{api_path}"
            body = {"dueDate": du_parser.isoparse(str(legacy_loan.due_date)).isoformat()}
            req = requests.post(
                api_url, headers=self.folio_client.okapi_headers, data=json.dumps(body)
            )
            if req.status_code == 422:
                error_message = json.loads(req.text)["errors"][0]["message"]
                self.migration_report.add(
                    Blurbs.Details, f"Change due date error: {error_message}"
                )
                logging.info(
                    f"{error_message}\t",
                )
                self.migration_report.add(Blurbs.Details, error_message)
                return False
            elif req.status_code == 201:
                self.migration_report.add(
                    Blurbs.Details, f"Successfully changed due date ({req.status_code})"
                )
                return True, json.loads(req.text), None
            elif req.status_code == 204:
                self.migration_report.add(
                    Blurbs.Details, f"Successfully changed due date ({req.status_code})"
                )
                return True, None, None
            else:
                self.migration_report.add(
                    Blurbs.Details,
                    f"Update open loan error http status: {req.status_code}",
                )
                req.raise_for_status()
        except HTTPError as exception:
            logging.info(
                f"{req.status_code} POST FAILED Change Due Date to {api_url}\t{json.dumps(body)})"
            )
            traceback.print_exc()
            logging.info(exception)
            return False, None, None


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
