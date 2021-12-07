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

from migration_tools.circulation_helper import (
    CirculationHelper,
    LegacyLoan,
    LegacyRequest,
)
from migration_tools.migration_tasks.custom_dict import InsensitiveDictReader
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from migration_tools.migration_configuration import MigrationConfiguration


class OpenRequestsMigrator(MigrationTaskBase):
    """Migrates Open Loans using the various Business logic apis for Circulation"""

    def __init__(self, configuration: MigrationConfiguration):
        super().__init__(configuration)
        self.circulation_helper = CirculationHelper(self.folio_client, "")
        csv.register_dialect("tsv", delimiter="\t")
        self.valid_legacy_requests = []
        file_path = os.path.join(
            self.results_folder, f'test_{time.strftime("%Y%m%d-%H%M%S")}.tsv'
        )
        with open(file_path, "w+", encoding="utf-8") as test_file:
            test_file.write("test")
        with open(args.open_requests_file, "r", encoding="utf-8") as requests_file:
            self.valid_legacy_requests = list(
                self.load_and_validate_legacy_requests(
                    InsensitiveDictReader(requests_file, dialect="tsv")
                )
            )
            logging.info(
                f"Loaded and validated {len(self.valid_legacy_requests)} requests in file"
            )
        self.patron_item_combos = set()
        self.t0 = time.time()
        self.num_duplicate_requests = 0
        self.skipped_since_already_added = 0
        self.processed_items = set()
        self.num_legacy_requests_processed = 0
        self.failed_requests: set = set()

        self.starting_point = 0  # TODO: Set as arg
        logging.info("Init completed")

    def do_work(self):
        logging.info("Starting")

        if self.starting_point > 0:
            logging.info(f"Skipping {self.starting_point} records")

        for num_requests, legacy_request in enumerate(
            self.valid_legacy_requests[self.starting_point :]
        ):
            t0_migration = time.time()
            self.add_stats("Processed requests")
            try:
                patron = self.circulation_helper.get_user_by_barcode(
                    legacy_request.patron_barcode
                )
                if not patron:
                    logging.error(
                        f"No user with barcode {legacy_request.patron_barcode} found in FOLIO"
                    )
                    self.add_stats("No user with barcode")
                    self.failed_requests.add(legacy_request)
                item = self.circulation_helper.get_item_by_barcode(
                    legacy_request.item_barcode
                )
                if not item:
                    logging.error(
                        f"No item with barcode {legacy_request.item_barcode} found in FOLIO"
                    )
                    self.add_stats("No item with barcode")
                    self.failed_requests.add(legacy_request)
                if patron and item:
                    legacy_request.patron_id = patron.get("id")
                    legacy_request.item_id = item.get("id")
                    if item["status"]["name"] in [
                        "Available",
                        "Aged to lost",
                        "Missing",
                    ]:
                        legacy_request.request_type = "Page"
                        logging.info(
                            f'Setting request to Page, since the status is {item["status"]["name"]}'
                        )
                    # we have everything we need. Let's get to it.
                    res_request = self.circulation_helper.create_request(
                        self.folio_client, legacy_request
                    )
                    if res_request:
                        # logging.info(item["status"]["name"])
                        self.add_stats(f'Successful statuses: {item["status"]["name"]}')
                        self.add_stats("Successfully processed requests")
                        logging.info("Successfully processed requests")
                    else:
                        logging.error(
                            f'Unsuccessfully processed requests.ItemBarcode {legacy_request.item_barcode} Status: {item["status"]["name"]}'
                        )
                        self.add_stats(
                            f'Unsuccessful statuses: {item["status"]["name"]}'
                        )
                        self.add_stats("Unsuccessfully processed requests")
                        self.failed_requests.add(legacy_request)

            except Exception as ee:  # Catch other exceptions than HTTP errors
                logging.info(
                    f"Error in row {num_requests}  Item barcode: {legacy_request.item_barcode} "
                    f"Patron barcode: {legacy_request.patron_barcode} {ee}"
                )
                traceback.print_exc()
                raise ee
            if num_requests % 25 == 0:
                self.print_dict_to_md_table(self.stats)
                logging.info(
                    f"SUMMARY: {timings(self.t0, t0_migration, num_requests)} {num_requests}"
                )
        self.wrap_up()

    @staticmethod
    @abstractmethod
    def add_arguments(parser):
        MigrationTaskBase.add_common_arguments(parser)
        MigrationTaskBase.add_argument(
            parser, "open_requests_file", help="File to TSV file containing Open Loans"
        )
        MigrationTaskBase.add_argument(
            parser, "client_folder", "Must contain a results and a reports folder"
        )

    def load_and_validate_legacy_requests(self, loans_reader):
        num_bad = 0
        barcodes = set()
        duplicate_barcodes = set()
        logging.info("Validating legacy loans in file...")
        for legacy_request_count, legacy_request_dict in enumerate(loans_reader):
            try:
                legacy_request = LegacyRequest(
                    legacy_request_dict, legacy_request_count
                )
                if any(legacy_request.errors):
                    num_bad += 1
                    for error in legacy_request.errors:
                        self.add_to_migration_report(error[0], error[1])
                else:
                    yield legacy_request
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_request_count} legacy requests with {num_bad} rotten apples"
        )

    def wrap_up(self):
        logging.info(json.dumps(list(self.circulation_helper.missing_patron_barcodes)))
        logging.info(json.dumps(list(self.circulation_helper.missing_item_barcodes)))

        logging.info("## Request migration counters")
        logging.info("Title | Number")
        logging.info("--- | ---:")
        logging.info(f"Total Rows in file  | {self.num_legacy_requests_processed}")
        super().wrap_up()
        self.circulation_helper.wrap_up()
        file_path = os.path.join(
            self.results_folder, f'failed_requests_{time.strftime("%Y%m%d-%H%M%S")}.tsv'
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
            for failed in self.failed_requests:
                writer.writerow(failed.to_dict())
        logging.info(json.dumps(list(self.failed_requests), sort_keys=True, indent=4))

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
