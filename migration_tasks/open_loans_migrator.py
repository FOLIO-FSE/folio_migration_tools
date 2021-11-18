import copy
import csv
import json
import logging
import os.path
import time
import traceback
from abc import abstractmethod
from datetime import datetime as dt, timedelta

import requests
from dateutil import parser as du_parser
from requests import HTTPError

from migration_tasks.migration_task_base import MigrationTaskBase

from migration_tasks.circulation_helper import (
    CirculationHelper,
    LegacyLoan,
    TransactionResult,
)
from migration_tasks.custom_dict import InsensitiveDictReader
from migration_tools.migration_configuration import MigrationConfiguration


class OpenLoansMigrator(MigrationTaskBase):
    """Migrates Open Loans using the various Business logic apis for Circulation"""

    def __init__(self, configuration: MigrationConfiguration):
        super().__init__(configuration)
        self.client_folder = configuration.client_folder
        self.reports_folder = os.path.join(self.client_folder, "reports")
        self.results_folder = os.path.join(self.client_folder, "results")

        self.service_point_id = configuration.service_point_id
        self.circulation_helper = CirculationHelper(folio_client, self.service_point_id)
        csv.register_dialect("tsv", delimiter="\t")
        self.valid_legacy_loans = []
        file_path = os.path.join(
            self.results_folder, f'test_{time.strftime("%Y%m%d-%H%M%S")}.tsv'
        )
        with open(file_path, "w+", encoding="utf-8") as test_file:
            test_file.write("test")
        with open(configuration.open_loans_file, "r", encoding="utf-8") as loans_file:
            self.valid_legacy_loans = list(
                self.load_and_validate_legacy_loans(
                    InsensitiveDictReader(loans_file, dialect="tsv")
                )
            )
            logging.info(
                f"Loaded and validated {len(self.valid_legacy_loans)} loans in file"
            )
        self.patron_item_combos = set()
        self.t0 = time.time()
        self.num_duplicate_loans = 0
        self.skipped_since_already_added = 0
        self.processed_items = set()
        self.failed = {}
        self.num_legacy_loans_processed = 0
        self.failed_and_not_dupe = {}

        self.starting_point = 0  # TODO: Set as arg
        logging.info("Init completed")

    def do_work(self):
        logging.info("Starting")

        if self.starting_point > 0:
            logging.info(f"Skipping {self.starting_point} records")
        for num_loans, legacy_loan in enumerate(
            self.valid_legacy_loans[self.starting_point :]
        ):
            t0_migration = time.time()
            self.add_stats("Processed loans")
            try:
                res_checkout = (
                    self.circulation_helper.check_out_by_barcode_override_iris(
                        legacy_loan
                    )
                )
                self.add_stats(res_checkout.migration_report_message)

                if not res_checkout.was_successful:
                    res_checkout = self.handle_checkout_failure(
                        legacy_loan, res_checkout
                    )
                    if not res_checkout.was_successful:
                        self.add_stats("Loan failed a second time")
                        logging.info(
                            f"Loan failed a second time. Item barcode {legacy_loan.item_barcode}"
                        )
                        if legacy_loan.item_barcode not in self.failed:
                            self.failed[legacy_loan.item_barcode] = legacy_loan
                        continue
                    else:
                        self.add_stats("Successfully checked out the second time")
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
                logging.info(
                    f"Error in row {num_loans}  Item barcode: {legacy_loan.item_barcode} "
                    f"Patron barcode: {legacy_loan.patron_barcode} {ee}"
                )
                traceback.print_exc()
                raise ee

        self.wrap_up()

    def handle_checkout_failure(self, legacy_loan, folio_checkout):
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
            return TransactionResult(True, "", "", "")
        elif folio_checkout.error_message.startswith("Aged to lost for item"):
            logging.debug("Setting Available")
            legacy_loan.next_item_status = "Available"
            self.set_item_status(legacy_loan)
            res_checkout = self.circulation_helper.check_out_by_barcode_override_iris(
                legacy_loan
            )
            legacy_loan.next_item_status = "Aged to lost"
            self.set_item_status(legacy_loan)
            logging.debug(
                f"Successfully Checked out Aged to lost item and put the status back"
            )
            return res_checkout
        elif folio_checkout.error_message == "Declared lost":
            return folio_checkout
        elif folio_checkout.error_message.startswith(
            "Cannot check out to inactive user"
        ):
            logging.info(
                "Cannot check out to inactive user. Activating and trying again"
            )
            user = self.get_user_by_barcode(legacy_loan.patron_barcode)
            expiration_date = user.get("expirationDate", dt.isoformat(dt.now()))
            user["expirationDate"] = dt.isoformat(dt.now() + timedelta(days=1))
            self.activate_user(user)
            logging.debug("Successfully Activated user")
            res = self.circulation_helper.check_out_by_barcode_override_iris(
                legacy_loan
            )  # checkout_and_update
            self.add_stats(res.migration_report_message)
            self.deactivate_user(user, expiration_date)
            logging.debug("Successfully Deactivated user again")
            self.add_stats("Handled inactive users")
            return res
        else:
            self.add_stats(f"Other checkout failure: {folio_checkout.error_message}")
            # First failure. Add to list of failed loans
            if legacy_loan.item_barcode not in self.failed:
                self.failed[legacy_loan.item_barcode] = legacy_loan
            # Second Failure. For duplicate rows. Needs cleaning...
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
                    f"Duplicate loans (or failed twice) item barcode"
                    f"{legacy_loan.item_barcode} patron barcode: {legacy_loan.patron_barcode}"
                )
                self.add_stats(f"Duplicate loans (or failed twice)")
                del self.failed[legacy_loan.item_barcode]
            return TransactionResult(False, "", "", "")

    @staticmethod
    @abstractmethod
    def add_arguments(parser):
        MigrationTaskBase.add_common_arguments(parser)
        MigrationTaskBase.add_argument(
            parser, "open_loans_file", help="File to TSV file containing Open Loans"
        )
        MigrationTaskBase.add_argument(
            parser, "service_point_id", "Id of the service point where checkout occurs"
        )
        MigrationTaskBase.add_argument(
            parser, "client_folder", "Must contain a results and a reports folder"
        )

    def load_and_validate_legacy_loans(self, loans_reader):
        num_bad = 0
        barcodes = set()
        duplicate_barcodes = set()
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

    def wrap_up(self):
        # wrap up
        for k, v in self.failed.items():
            self.failed_and_not_dupe[k] = [v.to_dict()]

        logging.info("## Loan migration counters")
        logging.info("Title | Number")
        logging.info("--- | ---:")
        logging.info(f"Failed items/loans | {len(self.failed_and_not_dupe)}")
        logging.info(f"Total Rows in file  | {self.num_legacy_loans_processed}")
        super().wrap_up()
        self.circulation_helper.wrap_up()
        file_path = os.path.join(
            self.results_folder, f'failed_loans_{time.strftime("%Y%m%d-%H%M%S")}.tsv'
        )
        csv_columns = [
            "due_date",
            "item_barcode",
            "next_item_status",
            "out_date",
            "patron_barcode",
            "renewal_count",
        ]
        with open(file_path, "w+") as failed_loans_file:
            writer = csv.DictWriter(
                failed_loans_file, fieldnames=csv_columns, dialect="tsv"
            )
            writer.writeheader()
            for k, failed_loan in self.failed_and_not_dupe.items():
                writer.writerow(failed_loan[0])
        logging.info(json.dumps(self.failed_and_not_dupe, sort_keys=True, indent=4))

    def handle_previously_failed_loans(self, loan):
        if loan["item_id"] in self.failed:
            logging.info(
                f"Loan succeeded but failed previously. Removing from failed    "
            )
            # this loan har previously failed. It can now be removed from failures:
            del self.failed[loan["item_id"]]

    def update_open_loan(self, folio_loan, legacy_loan: LegacyLoan):
        due_date = du_parser.isoparse(str(legacy_loan.due_date))
        out_date = du_parser.isoparse(str(legacy_loan.out_date))
        renewal_count = legacy_loan.renewal_count
        # TODO: add logging instead of print out
        t0_function = time.time()
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
                self.add_stats(
                    f"Update open loan error: {error_message} {req.status_code}"
                )
                return False
            elif req.status_code == 201:
                self.add_stats(f"Successfully updated open loan ({req.status_code})")
                return True, json.loads(req.text), None
            elif req.status_code == 204:
                self.add_stats(f"Successfully updated open loan ({req.status_code})")
                return True, None, None
            else:
                self.add_stats(f"Update open loan error http status: {req.status_code}")
                req.raise_for_status()
            logging.debug(f"Updating open loan was successful")
            return True
        except HTTPError as exception:
            logging.error(
                f"{req.status_code} PUT FAILED Extend loan to {loan_to_put['dueDate']}"
                f"\t {url}\t{json.dumps(loan_to_put)}"
            )
            traceback.print_exc()
            logging.error(exception)
            return False, None, None

    def handle_due_date_change_failure(self, legacy_loan, param):
        raise NotImplementedError()

    def handle_loan_update_failure(self, legacy_loan, param):
        raise NotImplementedError

    def declare_lost(self, folio_loan):
        declare_lost_url = f"/circulation/loans/{folio_loan['id']}/declare-item-lost"
        logging.debug(f"Declare lost url:{declare_lost_url}")
        due_date = du_parser.isoparse(folio_loan["dueDate"])
        data = {
            "declaredLostDateTime": dt.isoformat(due_date + timedelta(days=1)),
            "comment": "Created at migration. Date is due date + 1 day",
            "servicePointId": str(self.service_point_id),
        }
        logging.debug(f"Declare lost data: {json.dumps(data, indent=4)}")
        if self.folio_put_post(declare_lost_url, data, "POST", "Declare item as lost"):
            self.add_stats("Successfully declared loan as lost")
        else:
            logging.error(f"Unsuccessfully declared loan {folio_loan} as lost")
            self.add_stats("Unsuccessfully declared loan as lost")
        # TODO: Exception handling

    def claim_returned(self, folio_loan):
        claim_returned_url = (
            f"/circulation/loans/{folio_loan['id']}/claim-item-returned"
        )
        logging.debug(f"Claim returned url:{claim_returned_url}")
        due_date = du_parser.isoparse(folio_loan["dueDate"])
        data = {
            "itemClaimedReturnedDateTime": dt.isoformat(due_date + timedelta(days=1)),
            "comment": "Created at migration. Date is due date + 1 day",
        }
        logging.debug(f"Claim returned data:\t{json.dumps(data)}")
        if self.folio_put_post(
            claim_returned_url, data, "POST", "Declare item as lost"
        ):
            self.stats("Successfully declared loan as Claimed returned")
        else:
            logging.error(
                f"Unsuccessfully declared loan {folio_loan} as Claimed returned"
            )
            self.stats(f"Unsuccessfully declared loan {folio_loan} as Claimed returned")
        # TODO: Exception handling

    def set_item_status(self, legacy_loan: LegacyLoan):
        try:
            # Get Item by barcode, update status.
            item_url = f'{self.folio_client.okapi_url}/item-storage/items?query=(barcode=="{legacy_loan.item_barcode}")'
            resp = requests.get(item_url, headers=self.folio_client.okapi_headers)
            resp.raise_for_status()
            data = resp.json()
            folio_item = data["items"][0]
            folio_item["status"]["name"] = legacy_loan.next_item_status
            if self.update_item(folio_item):
                self.add_stats(
                    f"Successfully set item status to {legacy_loan.next_item_status}"
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
                self.add_stats(
                    f"Error setting item status to {legacy_loan.next_item_status}"
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
        self.add_stats("Successfully activated user")

    def deactivate_user(self, user, expiration_date):
        user["expirationDate"] = expiration_date
        user["active"] = False
        self.update_user(user)
        self.add_stats("Successfully deactivated user")

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
                self.add_stats(f"{action_description} error: {error_message}")
                resp.raise_for_status()
            elif resp.status_code in [201, 204]:
                self.add_stats(
                    f"Successfully {action_description} ({resp.status_code})"
                )
            else:
                self.add_stats(
                    f"{action_description} error. http status: {resp.status_code}"
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
            t0_function = time.time()
            api_url = f"{self.folio_client.okapi_url}/circulation/loans/{folio_loan['id']}/change-due-date"
            body = {
                "dueDate": du_parser.isoparse(str(legacy_loan.due_date)).isoformat()
            }
            req = requests.post(
                api_url, headers=self.folio_client.okapi_headers, data=json.dumps(body)
            )
            if req.status_code == 422:
                error_message = json.loads(req.text)["errors"][0]["message"]
                self.add_stats(f"Change due date error: {error_message}")
                logging.info(
                    f"{error_message}\t",
                )
                self.add_stats(error_message)
                return False
            elif req.status_code == 201:
                self.add_stats(f"Successfully changed due date ({req.status_code})")
                return True, json.loads(req.text), None
            elif req.status_code == 204:
                self.add_stats(f"Successfully changed due date ({req.status_code})")
                return True, None, None
            else:
                self.add_stats(f"Update open loan error http status: {req.status_code}")
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
