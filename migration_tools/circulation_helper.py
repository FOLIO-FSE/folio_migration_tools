import copy
import json
import logging
import re
import time
from datetime import datetime
from migration_tools.migration_report import MigrationReport
from migration_tools.report_blurbs import Blurbs
from migration_tools.transaction_migration.legacy_loan import LegacyLoan
from migration_tools.transaction_migration.transaction_result import TransactionResult
from migration_tools.transaction_migration.legacy_request import LegacyRequest

import requests
from folioclient import FolioClient
from requests import HTTPError

date_time_format = "%Y-%m-%dT%H:%M:%S.%f+0000"


class CirculationHelper:
    def __init__(
        self,
        folio_client: FolioClient,
        service_point_id,
        migration_report: MigrationReport,
    ):
        self.folio_client = folio_client
        self.service_point_id = service_point_id
        self.missing_patron_barcodes = set()
        self.missing_item_barcodes = set()
        self.migration_report: MigrationReport = migration_report

    def get_user_by_barcode(self, user_barcode):
        if user_barcode in self.missing_patron_barcodes:
            logging.info("User is already detected as missing")
            return {}
        user_path = f"/users?query=barcode=={user_barcode}"
        try:
            users = self.folio_client.folio_get(user_path, "users")
            if any(users):
                return users[0]
            self.missing_patron_barcodes.add(user_barcode)
            return {}
        except Exception as ee:
            logging.error(f"{ee} {user_path}")
            return {}

    def get_item_by_barcode(self, item_barcode):
        if item_barcode in self.missing_item_barcodes:
            logging.info("Item is already detected as missing")
            return {}
        item_path = f"/item-storage/items?query=barcode=={item_barcode}"
        try:
            item = self.folio_client.folio_get(item_path, "items")
            if any(item):
                return item[0]
            self.missing_item_barcodes.add(item_barcode)
            return {}
        except Exception as ee:
            logging.error(f"{ee} {item_path}")
            return {}

    def check_out_by_barcode(self, legacy_loan: LegacyLoan) -> TransactionResult:
        """Checks out a legacy loan using the Endpoint /circulation/check-out-by-barcode
        Adds all possible overrides in order to make the transaction go through
        Args:
            legacy_loan (LegacyLoan): Legacy loan to be posted

        Returns:
            TransactionResult: the result of the transaction
        """
        t0_function = time.time()
        data = {
            "itemBarcode": legacy_loan.item_barcode,
            "userBarcode": legacy_loan.patron_barcode,
            "loanDate": legacy_loan.out_date.isoformat(),
            "servicePointId": self.service_point_id,
            "overrideBlocks": {
                "itemNotLoanableBlock": {"dueDate": legacy_loan.due_date.isoformat()},
                "patronBlock": {},
                "itemLimitBlock": {},
                "comment": "Migrated from legacy system",
            },
        }
        path = "/circulation/check-out-by-barcode"
        url = f"{self.folio_client.okapi_url}{path}"
        try:
            if legacy_loan.patron_barcode in self.missing_patron_barcodes:
                error_message = "Patron barcode already detected as missing"
                logging.error(
                    f"{error_message} Patron barcode: {legacy_loan.patron_barcode} "
                    f"Item Barcode:{legacy_loan.item_barcode}"
                )
                return TransactionResult(
                    False, False, None, error_message, error_message
                )
            req = requests.post(
                url, headers=self.folio_client.okapi_headers, data=json.dumps(data)
            )
            if req.status_code == 422:
                error_message_from_folio = json.loads(req.text)["errors"][0]["message"]
                stat_message = error_message_from_folio
                error_message = error_message_from_folio
                if "has the item status" in error_message_from_folio:
                    stat_message = re.findall(
                        r"(?<=has the item status\s).*(?=\sand cannot be checked out)",
                        error_message_from_folio,
                    )[0]
                    error_message = (
                        f"{stat_message} for item with "
                        f"barcode {legacy_loan.item_barcode}"
                    )
                    return TransactionResult(
                        False,
                        True,
                        None,
                        error_message_from_folio,
                        stat_message,
                    )
                elif "No item with barcode" in error_message_from_folio:
                    error_message = (
                        f"No item with barcode {legacy_loan.item_barcode} in FOLIO"
                    )
                    stat_message = "Item barcode not in FOLIO"
                    self.missing_item_barcodes.add(legacy_loan.item_barcode)
                    return TransactionResult(
                        False,
                        False,
                        None,
                        error_message_from_folio,
                        stat_message,
                    )
                elif "find user with matching barcode" in error_message_from_folio:
                    self.missing_patron_barcodes.add(legacy_loan.patron_barcode)
                    error_message = (
                        f"No patron with barcode {legacy_loan.patron_barcode} in FOLIO"
                    )
                    stat_message = "Patron barcode not in FOLIO"
                    return TransactionResult(
                        False,
                        False,
                        None,
                        error_message_from_folio,
                        stat_message,
                    )
                elif (
                    "Cannot check out item that already has an open"
                    in error_message_from_folio
                ):
                    return TransactionResult(
                        False,
                        False,
                        None,
                        error_message_from_folio,
                        error_message_from_folio,
                    )
                logging.error(
                    f"{error_message} "
                    f"Patron barcode: {legacy_loan.patron_barcode} "
                    f"Item Barcode:{legacy_loan.item_barcode}"
                )
                self.migration_report.add(Blurbs.Details, stat_message)
                return TransactionResult(
                    False, True, None, error_message, f"Check out error: {stat_message}"
                )
            elif req.status_code == 201:
                stats = "Successfully checked out by barcode"
                logging.debug(
                    "%s (item barcode %s}) in %ss",
                    stats,
                    legacy_loan.item_barcode,
                    f"{(time.time() - t0_function):.2f}",
                )
                return TransactionResult(True, False, json.loads(req.text), None, stats)
            elif req.status_code == 204:
                stats = "Successfully checked out by barcode"
                logging.debug(
                    "%s (item barcode %s) %s",
                    stats,
                    legacy_loan.item_barcode,
                    req.status_code,
                )
                return TransactionResult(True, False, None, None, stats)
            else:
                req.raise_for_status()
        except HTTPError:
            logging.exception(
                f"{req.status_code}\tPOST FAILED "
                f"{url}\n\t{json.dumps(data)}\n\t{req.text}"
            )
            return TransactionResult(
                False,
                False,
                None,
                "5XX",
                f"Failed checkout http status {req.status_code}",
            )

    @staticmethod
    def create_request(
        folio_client: FolioClient,
        legacy_request: LegacyRequest,
    ):
        try:
            path = "/circulation/requests"
            url = f"{folio_client.okapi_url}{path}"
            data = legacy_request.to_dict()

            data["requestProcessingParameters"] = {
                "overrideBlocks": {
                    "itemNotLoanableBlock": {
                        "dueDate": legacy_request.request_expiration_date.isoformat()
                    },
                    "patronBlock": {},
                    "itemLimitBlock": {},
                    "comment": "Migrated from legacy system",
                }
            }
            req = requests.post(
                url, headers=folio_client.okapi_headers, data=json.dumps(data)
            )
            logging.debug(f"POST {req.status_code}\t{url}\t{json.dumps(data)}")
            if str(req.status_code) == "422":
                logging.error(
                    f"{json.loads(req.text)['errors'][0]['message']}\t{json.dumps(data)}"
                )
                return False
            else:
                req.raise_for_status()
                logging.info(
                    f"{req.status_code} Successfully created {legacy_request.request_type}"
                )
                return True
        except Exception as exception:
            logging.error(exception, exc_info=True)
            return False

    @staticmethod
    def extend_open_loan(
        folio_client: FolioClient, loan, extension_due_date, extend_out_date
    ):
        # TODO: add logging instead of print out
        try:
            loan_to_put = copy.deepcopy(loan)
            del loan_to_put["metadata"]
            loan_to_put["dueDate"] = extension_due_date.isoformat()
            loan_to_put["loanDate"] = extend_out_date.isoformat()
            url = f"{folio_client.okapi_url}/circulation/loans/{loan_to_put['id']}"

            req = requests.put(
                url, headers=folio_client.okapi_headers, data=json.dumps(loan_to_put)
            )
            logging.info(
                f"{req.status_code}\tPUT Extend loan {loan_to_put['id']} to {loan_to_put['dueDate']}\t {url}"
            )
            if str(req.status_code) == "422":
                logging.error(
                    f"{json.loads(req.text)['errors'][0]['message']}\t{json.dumps(loan_to_put)}"
                )
                return False
            else:
                req.raise_for_status()
                logging.info(f"{req.status_code} Successfully Extended loan")
            return True
        except Exception as exception:
            logging.error(
                f"PUT FAILED Extend loan to {loan_to_put['dueDate']}\t {url}\t{json.dumps(loan_to_put)}",
                exc_info=True,
            )
            return False

    @staticmethod
    def create_request_variant(
        folio_client: FolioClient,
        request_type,
        patron,
        item,
        service_point_id,
        request_date=datetime.now(),
    ):
        try:

            data = {
                "requestType": request_type,
                "fulfilmentPreference": "Hold Shelf",
                "requester": {"barcode": patron["barcode"]},
                "requesterId": patron["id"],
                "item": {"barcode": item["barcode"]},
                "itemId": item["id"],
                "pickupServicePointId": service_point_id,
                "requestDate": request_date.strftime(date_time_format),
            }
            path = "/circulation/requests"
            url = f"{folio_client.okapi_url}{path}"
            req = requests.post(
                url, headers=folio_client.okapi_headers, data=json.dumps(data)
            )
            logging.debug(f"POST {req.status_code}\t{url}\t{json.dumps(data)}")
            if str(req.status_code) == "422":
                logging.error(
                    f"{json.loads(req.text)['errors'][0]['message']}\t{json.dumps(data)}"
                )
            else:
                req.raise_for_status()
                logging.info(
                    f"POST {req.status_code} Successfully created request {request_type}"
                )
        except Exception as exception:
            logging.error(exception, exc_info=True)
