import copy
import json
import logging
import re
import time
from typing import Set

import httpx
import i18n
from folioclient import FolioClient
from httpx import HTTPError

from folio_migration_tools.helper import Helper
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.transaction_migration.legacy_loan import LegacyLoan
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest
from folio_migration_tools.transaction_migration.transaction_result import (
    TransactionResult,
)

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
        self.missing_patron_barcodes: Set[str] = set()
        self.missing_item_barcodes: Set[str] = set()
        self.migration_report: MigrationReport = migration_report

    def get_user_by_barcode(self, user_barcode):
        if user_barcode in self.missing_patron_barcodes:
            self.migration_report.add_general_statistics(
                i18n.t("Users already detected as missing")
            )
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
            self.migration_report.add_general_statistics(
                i18n.t("Items already detected as missing")
            )
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

    def is_checked_out(self, legacy_loan: LegacyLoan) -> bool:
        """Makes a deeper check to find out if the loan is already processed.
        Looks up the item id, and then searches Loan Storage for any open loans.
        If there are open loans, returns True. Else False.

        Args:
            legacy_loan (LegacyLoan): _description_

        Returns:
            bool: _description_
        """
        if item := self.get_item_by_barcode(legacy_loan.item_barcode):
            if self.get_active_loan_by_item_id(item["id"]):
                return True
        return False

    def get_active_loan_by_item_id(self, item_id: str) -> dict:
        """Queries FOLIO for the first open loan.

        Args:
            item_id (str): the item ID. A uuid string.

        Returns:
            dict: The open loan, if found. Else an empty dictionary
        """
        loan_path = f'/loan-storage/loans?query=(itemId=="{item_id}")'
        try:
            loans = self.folio_client.folio_get(loan_path, "loans")
            return next((loan for loan in loans if loan["status"]["name"] == "Open"), {})
        except Exception as ee:
            logging.error(f"{ee} {loan_path}")
            return {}

    def get_holding_by_uuid(self, holdings_uuid):
        holdings_path = f"/holdings-storage/holdings/{holdings_uuid}"
        try:
            return self.folio_client.folio_get_single_object(holdings_path)
        except Exception as ee:
            logging.error(f"{ee} {holdings_path}")
            return {}

    def check_out_by_barcode(self, legacy_loan: LegacyLoan) -> TransactionResult:
        """Checks out a legacy loan using the Endpoint /circulation/check-out-by-barcode
        Adds all possible overrides in order to make the transaction go through

        Args:
            legacy_loan (LegacyLoan): _description_

        Returns:
            TransactionResult: _description_
        """

        t0_function = time.time()
        data = {
            "itemBarcode": legacy_loan.item_barcode,
            "userBarcode": legacy_loan.patron_barcode,
            "loanDate": legacy_loan.out_date.isoformat(),
            "servicePointId": legacy_loan.service_point_id,
            "overrideBlocks": {
                "itemNotLoanableBlock": {"dueDate": legacy_loan.due_date.isoformat()},
                "patronBlock": {},
                "itemLimitBlock": {},
                "comment": "Migrated from legacy system",
            },
        }
        if legacy_loan.proxy_patron_barcode:
            data.update({"proxyUserBarcode": legacy_loan.proxy_patron_barcode})
        path = "/circulation/check-out-by-barcode"
        url = f"{self.folio_client.okapi_url}{path}"
        try:
            if legacy_loan.patron_barcode in self.missing_patron_barcodes:
                error_message = i18n.t("Patron barcode already detected as missing")
                logging.error(
                    f"{error_message} Patron barcode: {legacy_loan.patron_barcode} "
                    f"Item Barcode:{legacy_loan.item_barcode}"
                )
                return TransactionResult(False, False, "", error_message, error_message)
            req = httpx.post(url, headers=self.folio_client.okapi_headers, json=data, timeout=None)
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
                        f"{stat_message} for item with barcode {legacy_loan.item_barcode}"
                    )
                    return TransactionResult(
                        False,
                        True,
                        None,
                        error_message_from_folio,
                        stat_message,
                    )
                elif "No item with barcode" in error_message_from_folio:
                    error_message = f"No item with barcode {legacy_loan.item_barcode} in FOLIO"
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
                    error_message = f"No patron with barcode {legacy_loan.patron_barcode} in FOLIO"
                    stat_message = i18n.t("Patron barcode not in FOLIO")
                    return TransactionResult(
                        False,
                        False,
                        None,
                        error_message_from_folio,
                        stat_message,
                    )
                elif "Cannot check out item that already has an open" in error_message_from_folio:
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
                self.migration_report.add("Details", stat_message)
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
                return TransactionResult(True, False, json.loads(req.text), "", stats)
            elif req.status_code == 204:
                stats = "Successfully checked out by barcode"
                logging.debug(
                    "%s (item barcode %s) %s",
                    stats,
                    legacy_loan.item_barcode,
                    req.status_code,
                )
                return TransactionResult(True, False, None, "", stats)
            else:
                req.raise_for_status()
        except HTTPError:
            logging.exception(
                "%s\tPOST FAILED %s\n\t%s\n\t%s",
                req.status_code,
                url,
                json.dumps(data),
                req.text,
            )
            return TransactionResult(
                False,
                False,
                None,
                "5XX",
                i18n.t("Failed checkout http status %{code}", code=req.status_code),
            )

    @staticmethod
    def create_request(
        folio_client: FolioClient, legacy_request: LegacyRequest, migration_report: MigrationReport
    ):
        try:
            path = "/circulation/requests"
            url = f"{folio_client.okapi_url}{path}"
            data = legacy_request.serialize()
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
            req = httpx.post(url, headers=folio_client.okapi_headers, json=data, timeout=None)
            logging.debug(f"POST {req.status_code}\t{url}\t{json.dumps(data)}")
            if str(req.status_code) == "422":
                message = json.loads(req.text)["errors"][0]["message"]
                logging.error(f"{message}")
                migration_report.add_general_statistics(message)
                return False
            else:
                req.raise_for_status()
                logging.info(
                    "%s Successfully created %s",
                    req.status_code,
                    legacy_request.request_type,
                )
                return True
        except Exception as exception:
            logging.error(exception, exc_info=True)
            migration_report.add("Details", exception)
            Helper.log_data_issue(
                legacy_request.item_barcode,
                exception,
                json.dumps(legacy_request.to_source_dict()),
            )
            return False

    def load_migrated_user_barcodes(self, user_barcodes, patron_files, folder_structure):
        if any(patron_files):
            for filedef in patron_files:
                my_path = folder_structure.results_folder / filedef.file_name
                with open(my_path) as patron_file:
                    for row in patron_file:
                        rec = json.loads(row)
                        user_barcodes.add(rec.get("barcode", "None"))
            logging.info("Loaded %s barcodes from users", len(user_barcodes))

    def load_migrated_item_barcodes(self, item_barcodes, item_files, folder_structure):
        if any(item_files):
            for filedef in item_files:
                my_path = folder_structure.results_folder / filedef.file_name
                with open(my_path) as item_file:
                    for row in item_file:
                        rec = json.loads(row)
                        item_barcodes.add(rec.get("barcode", "None"))
            logging.info("Loaded %s barcodes from items", len(item_barcodes))

    @staticmethod
    def extend_open_loan(folio_client: FolioClient, loan, extension_due_date, extend_out_date):
        try:
            loan_to_put = copy.deepcopy(loan)
            del loan_to_put["metadata"]
            loan_to_put["dueDate"] = extension_due_date.isoformat()
            loan_to_put["loanDate"] = extend_out_date.isoformat()
            url = f"{folio_client.okapi_url}/circulation/loans/{loan_to_put['id']}"

            req = httpx.put(
                url, headers=folio_client.okapi_headers, json=loan_to_put, timeout=None
            )
            logging.info(
                "%s\tPUT Extend loan %s to %s\t %s",
                req.status_code,
                loan_to_put["id"],
                loan_to_put["dueDate"],
                url,
            )
            if str(req.status_code) == "422":
                logging.error(
                    "%s\t%s",
                    json.loads(req.text)["errors"][0]["message"],
                    json.dumps(loan_to_put),
                )
                return False
            else:
                req.raise_for_status()
                logging.info("%s Successfully Extended loan", req.status_code)
            return True
        except Exception:
            logging.exception(
                "PUT FAILED Extend loan to %s\t %s\t%s",
                loan_to_put["dueDate"],
                url,
                json.dumps(loan_to_put),
            )
            return False
