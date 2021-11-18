import csv
import json
import logging
import time
import uuid
from abc import abstractmethod

import requests
from requests import HTTPError

from migration_tasks.circulation_helper import LegacyFeeFine
from migration_tasks.custom_dict import InsensitiveDictReader
from migration_tasks.migration_task_base import MigrationTaskBase
from migration_tools.migration_configuration import MigrationConfiguration


class MigrateFeesAndFines(MigrationTaskBase):
    """Migrates fees and fines"""

    def __init__(self, configuration: MigrationConfiguration):
        super().__init__(configuration)

        csv.register_dialect("tsv", delimiter="\t")
        self.valid_legacy_fees = []
        with open(configuration.fees_file, "r") as fees_file:
            self.valid_legacy_fees = list(
                self.load_and_validate_legacy_fees(
                    InsensitiveDictReader(fees_file, dialect="tsv")
                )
            )
            logging.info(
                f"Loaded and validated {len(self.valid_legacy_fees)} fees in file"
            )
        self.t0 = time.time()
        self.num_duplicate_loans = 0
        self.skipped_since_already_added = 0
        self.processed_items = set()
        self.failed = []
        self.missing_user_barcodes = set()
        self.missing_items_barcodes = set()
        self.num_legacy_loans_processed = 0
        self.failed_and_not_dupe = {}
        # magic strings
        self.fee_fine_owner_name = "CUL"
        self.fee_fine_owner_id = "9898dd87-43dc-4c2b-ae55-2d8b66eb32ee"
        self.lost_fee_fine = "cf238f9f-7018-47b7-b815-bb2db798e19f"
        self.starting_point = 0  # TODO: Set as arg
        logging.info("Init completed")

    def do_work(self):
        if self.starting_point > 0:
            logging.info(f"Skipping {self.starting_point} records")
        for num_fees, legacy_fee in enumerate(
            self.valid_legacy_fees[self.starting_point :]
        ):
            t0_migration = time.time()
            try:
                # Get item, by barcode
                patron_item = {
                    "user": self.get_user(legacy_fee.patron_barcode),
                    "item": self.get_item(legacy_fee.item_barcode),
                }
                if not patron_item["user"]:
                    self.add_stats("User barcode not found")
                    self.missing_user_barcodes.add(legacy_fee.patron_barcode)
                    raise ValueError(
                        f"User not found with barcode {legacy_fee.patron_barcode}"
                    )
                if not patron_item["item"]:
                    self.add_stats("Item barcode not found")
                    self.missing_items_barcodes(legacy_fee.item_barcode)
                    # self.add_to_migration_report("Item barcodes not found in FOLIO", legacy_fee.item_barcode)
                # Post this: /accounts
                account_id = str(uuid.uuid4())
                post_account_path = f"{self.folio_client.okapi_url}/accounts"
                account_payload = {
                    "amount": legacy_fee.amount,
                    "remaining": legacy_fee.remaining,
                    "status": {"name": "Open"},  # values used are Open and Closed
                    "paymentStatus": {
                        "name": "Outstanding"
                        # Outstanding, Paid partially, Paid fully, Waived partially, Waived fully, Transferred partially, Transferred fully, Refunded partially, Refunded fully, Cancelled as error
                    },
                    "feeFineType": type_map(legacy_fee.fee_fine_type)[1],
                    "feeFineOwner": self.fee_fine_owner_name,
                    "title": patron_item.get("item", {}).get("title", ""),
                    "callNumber": patron_item.get("item", {}).get("callNumber", ""),
                    "barcode": patron_item.get("item", {}).get("barcode", ""),
                    "materialType": patron_item.get("item", {})
                    .get("materialType", {})
                    .get("name", ""),
                    "location": patron_item.get("item", {})
                    .get("effectiveLocation", {})
                    .get("name", ""),
                    "metadata": self.folio_client.get_metadata_construct(),
                    "userId": patron_item.get("user", {}).get("id", ""),
                    "itemId": patron_item.get("item", {}).get("id", ""),
                    "materialTypeId": patron_item.get("item", {})
                    .get("materialType", {})
                    .get("id", ""),
                    "feeFineId": type_map(legacy_fee.fee_fine_type)[0],
                    "ownerId": self.fee_fine_owner_id,
                    "id": account_id,
                }
                logging.debug(json.dumps(account_payload))
                self.post_stuff(
                    post_account_path, account_payload, legacy_fee.source_dict
                )

                # Post this to /feefineactions?query=(userId==895412c6-62b4-4680-bdcf-7c875f11dd5b)&limit=10000
                fee_fine_action_path = f"{self.folio_client.okapi_url}/feefineactions?query=(userId=={patron_item.get('user', {})['id']})&limit=10000"
                feefine_action_payload = {
                    "dateAction": legacy_fee.created_date.isoformat(),
                    "typeAction": type_map(legacy_fee.fee_fine_type)[1],
                    "comments": stringify_source_record(legacy_fee.source_dict),
                    "notify": False,  # For sure
                    "amountAction": legacy_fee.amount,
                    "balance": legacy_fee.remaining,
                    "transactionInformation": "",
                    "createdAt": "0e1de5af-8fc2-44e8-8aec-246dbea9c09b",
                    "source": self.folio_client.username,
                    "accountId": account_id,
                    "userId": patron_item.get("user", {}).get("id", ""),
                    "id": str(uuid.uuid4()),
                }
                logging.debug(json.dumps(feefine_action_payload))
                self.post_stuff(
                    fee_fine_action_path, feefine_action_payload, legacy_fee.source_dict
                )
            except ValueError as ve:
                logging.error(
                    f"ValueError in row {num_fees}  Item id: {legacy_fee.item_barcode}"
                    f"Patron id: {legacy_fee.patron_barcode} {ve}"
                )
                self.add_to_migration_report("General", "ValueErrors")
            except Exception as ee:  # Catch other exceptions than HTTP errors
                logging.error(
                    f"Error in row {num_fees}  Item id: {legacy_fee.item_barcode}"
                    f"Patron id: {legacy_fee.patron_barcode} {ee}"
                )
                self.add_to_migration_report("General", "Exceptions")
            finally:
                if num_fees % 50 == 0:
                    self.print_dict_to_md_table(self.stats)
                    self.print_migration_report()
                    logging.info(
                        f"{timings(self.t0, t0_migration, num_fees)} {num_fees}"
                    )
        self.wrap_up()

    def get_item(self, item_barcode):
        if item_barcode in self.missing_items_barcodes:
            self.add_to_migration_report(
                "Item barcodes reported as misssing", item_barcode
            )
            return {}
        item_path = f"/inventory/items?query=barcode=={item_barcode}"
        try:
            items = self.folio_client.folio_get(item_path, "items")
            if any(items):
                return items[0]
            self.add_to_migration_report("General", "No item found by barcode")
            return {}
        except Exception as ee:
            logging.error(f"{ee} {item_path}")
            return {}

    def get_user(self, user_barcode):
        if user_barcode in self.missing_user_barcodes:
            self.add_to_migration_report(
                "User barcodes reported as misssing", user_barcode
            )
            return {}
        user_path = f"/users?query=barcode=={user_barcode}"
        try:
            users = self.folio_client.folio_get(user_path, "users")
            if any(users):
                return users[0]
            self.add_to_migration_report("General", "No user found by barcode")
            return {}
        except Exception as ee:
            logging.error(f"{ee} {user_path}")
            return {}

    def post_stuff(self, request_path: str, payload: dict, source_data: dict):
        try:
            resp = requests.post(
                request_path,
                headers=self.folio_client.okapi_headers,
                data=json.dumps(payload),
            )
            logging.info(f"HTTP {resp.status_code} {request_path}")
            self.add_to_migration_report("HTTP Codes", resp.status_code)
            resp.raise_for_status()
        except HTTPError as httpError:
            self.add_to_migration_report("Failed requests", httpError)
            logging.error(httpError)
            logging.info(resp.text)

    @staticmethod
    @abstractmethod
    def add_arguments(parser):
        MigrationTaskBase.add_common_arguments(parser)
        MigrationTaskBase.add_argument(
            parser, "fees_file", help="File to TSV file containing Open Loans"
        )

    def load_and_validate_legacy_fees(self, fees_reader):
        num_bad = 0
        logging.info("Validating legacy fees in file...")
        for legacy_fees_count, legacy_fee_dict in enumerate(fees_reader):
            try:
                fee_fine = LegacyFeeFine(legacy_fee_dict, legacy_fees_count)
                if any(fee_fine.errors):
                    num_bad += 1
                    for error in fee_fine.errors:
                        self.add_to_migration_report(error[0], error[1])
                else:
                    yield fee_fine
            except ValueError as ve:
                logging.exception(ve)
        logging.info(
            f"Done validating {legacy_fees_count} legacy FeeFines with {num_bad} rotten apples"
        )

    def wrap_up(self):
        # wrap up
        super().wrap_up()


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average object/sec: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )


def stringify_source_record(legacy_fee):
    ret_str = "THIS RECORD WAS MIGRATED FROM VOYAGER.\nThis is the original data\n"
    for key, value in legacy_fee.items():
        ret_str += f"{key}: {value}\n"
    return ret_str


def type_map(type_code):
    m = {
        "": "",
        "1": ("9523cb96-e752-40c2-89da-60f3961a488d", "Overdue fine"),  # Overdue
        "2": (
            "6c6d86d1-f3af-42a1-8d3a-86df9a4c8408",
            "Lost item replacement",
        ),  # Lost Item Replacement
        "3": (
            "c7dede15-aa48-45ed-860b-f996540180e0",
            "Lost item processing fee",
        ),  # Lost Item Processing
        "4": ("", ""),  # Media Booking Late Charge
        "5": ("", ""),  # Media Booking Usage Fee
        "6": (
            "6c6d86d1-f3af-42a1-8d3a-86df9a4c8408",
            "Lost item replacement",
        ),  # Equipment Replacement
        "7": (
            "c7dede15-aa48-45ed-860b-f996540180e0",
            "Lost item processing fee",
        ),  # Lost Equipment Processing
        "8": ("", ""),  # Accrued Fine
        "9": ("", ""),  # Accrued Demerit
        "10": ("", ""),  # Demerit
        "11": ("", ""),  # Recall
        "12": (
            "fce8f9e1-8e1d-4457-bda1-e85d4854a734",
            "Damaged",
        ),  # Damage & Repair Charge
        "13": ("", ""),  # Binding Charge
        "14": ("", ""),  # Miscellaneous Charge
        "15": ("", ""),  # Added by Fee Load
        "16": ("", ""),  # Added by Fee Load
        "17": ("", ""),  # Added by Fee Load
        "18": ("", ""),  # Added by Fee Load
        "19": ("", ""),  # Added by Fee Load
        "20": ("", ""),  # Added by Fee Load
    }
    return m[type_code]


def trans_type(transaction_type_code):
    tmap = {
        "1": "Payment",
        "2": "Forgive",
        "3": "Error",
        "4": "Refund",
        "5": "Bursar Transfer",
        "6": "Bursar Refund",
        "7": "Bursar Refund--Transfered",
        "8": "Suspension",
        "9": "Batch Forgive",
        "10": "E-Kiosk",
    }
    return tmap[transaction_type_code]


def payment_method(payment_type_code):
    pmap = {
        "1": "Cash",
        "2": "Check",
        "3": "Credit Card",
        "4": "Sent to CU Collections",
    }
    return pmap[payment_type_code]


"""
{
  "feefines" : [ {
    "automatic" : true,
    "feeFineType" : "Overdue fine",
    "id" : "9523cb96-e752-40c2-89da-60f3961a488d"
  }, {
    "automatic" : true,
    "feeFineType" : "Replacement processing fee",
    "id" : "d20df2fb-45fd-4184-b238-0d25747ffdd9"
  }, {
    "automatic" : true,
    "feeFineType" : "Lost item fee",
    "id" : "cf238f9f-7018-47b7-b815-bb2db798e19f"
  }, {
    "automatic" : true,
    "feeFineType" : "Lost item processing fee",
    "id" : "c7dede15-aa48-45ed-860b-f996540180e0"
  }, {
    "automatic" : false,
    "feeFineType" : "Damaged",
    "ownerId" : "9898dd87-43dc-4c2b-ae55-2d8b66eb32ee",
    "metadata" : {
      "createdDate" : "2021-04-29T17:34:11.744+00:00",
      "createdByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc",
      "updatedDate" : "2021-04-29T17:34:11.744+00:00",
      "updatedByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc"
    },
    "id" : "fce8f9e1-8e1d-4457-bda1-e85d4854a734"
  }, {
    "automatic" : false,
    "feeFineType" : "Library card",
    "ownerId" : "9898dd87-43dc-4c2b-ae55-2d8b66eb32ee",
    "metadata" : {
      "createdDate" : "2021-04-29T17:34:39.316+00:00",
      "createdByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc",
      "updatedDate" : "2021-04-29T17:34:39.316+00:00",
      "updatedByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc"
    },
    "id" : "4e30d2ba-d90d-4ab4-bbc6-cea7e01427d4"
  }, {
    "automatic" : false,
    "feeFineType" : "Lost item replacement",
    "ownerId" : "9898dd87-43dc-4c2b-ae55-2d8b66eb32ee",
    "metadata" : {
      "createdDate" : "2021-04-29T17:34:48.160+00:00",
      "createdByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc",
      "updatedDate" : "2021-04-29T17:34:48.160+00:00",
      "updatedByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc"
    },
    "id" : "6c6d86d1-f3af-42a1-8d3a-86df9a4c8408"
  }, {
    "automatic" : false,
    "feeFineType" : "Overdue",
    "ownerId" : "9898dd87-43dc-4c2b-ae55-2d8b66eb32ee",
    "metadata" : {
      "createdDate" : "2021-04-29T17:34:54.040+00:00",
      "createdByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc",
      "updatedDate" : "2021-04-29T17:34:54.040+00:00",
      "updatedByUserId" : "a2959cda-1338-40ba-ad9e-368fc4cbf1cc"
    },
    "id" : "ea0923bd-1ba4-485a-a494-73e86ba260b5"
  }, {
    "automatic" : true,
    "feeFineType" : "Lost item fee (actual cost)",
    "id" : "73785370-d3bd-4d92-942d-ae2268e02ded"
  } ],
  "totalRecords" : 9,
  "resultInfo" : {
    "totalRecords" : 9,
    "facets" : [ ],
    "diagnostics" : [ ]
  }"""
