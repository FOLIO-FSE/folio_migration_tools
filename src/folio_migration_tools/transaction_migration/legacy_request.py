import datetime
import logging
import uuid
from zoneinfo import ZoneInfo

from dateutil import tz
from dateutil.parser import parse

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError

utc = ZoneInfo("UTC")


class LegacyRequest(object):
    def __init__(self, legacy_request_dict, tenant_timezone=utc, row=0):
        # validate
        correct_headers = [
            "item_barcode",
            "patron_barcode",
            "request_date",
            "request_expiration_date",
            "comment",
            "request_type",
            "pickup_servicepoint_id",
        ]
        self.errors = []

        for prop in correct_headers:
            if prop not in legacy_request_dict:
                self.errors.append(("Missing properties in legacy data", prop))
            if prop != "comment" and not legacy_request_dict[prop].strip():
                self.errors.append(("Empty properties in legacy data", prop))

        self.item_barcode = legacy_request_dict["item_barcode"].strip()
        self.patron_id = ""
        self.tenant_timezone = tenant_timezone
        self.item_id = ""
        self.instance_id = ""
        self.holdings_record_id = ""
        self.patron_barcode = legacy_request_dict["patron_barcode"].strip()
        self.comment = legacy_request_dict["comment"].strip()
        self.request_type = legacy_request_dict["request_type"].strip()
        self.pickup_servicepoint_id = legacy_request_dict["pickup_servicepoint_id"].strip()
        self.fulfillment_preference = "Hold Shelf"

        if self.request_type not in ["Hold", "Recall", "Page"]:
            self.errors.append((f"{self.request_type} not allowd", "request_type"))

        try:
            temp_request_date: datetime.datetime = parse(legacy_request_dict["request_date"])
            if temp_request_date.tzinfo != tz.UTC:
                temp_request_date = temp_request_date.replace(tzinfo=self.tenant_timezone)
        except Exception:
            self.errors.append(("Parse date failure. Setting UTC NOW", "request_date"))
            temp_request_date = datetime.now(ZoneInfo("UTC"))
        try:
            temp_expiration_date: datetime.datetime = parse(
                legacy_request_dict["request_expiration_date"]
            )
            if temp_expiration_date.tzinfo != tz.UTC:
                temp_expiration_date = temp_expiration_date.replace(tzinfo=self.tenant_timezone)
        except Exception:
            temp_expiration_date = datetime.now(ZoneInfo("UTC"))
            self.errors.append(("Parse date failure. Setting UTC NOW", "request_expiration_date"))
        if temp_expiration_date.hour == 0 and temp_expiration_date.minute == 0:
            temp_expiration_date = temp_expiration_date.replace(hour=23, minute=59)

        self.request_date: datetime.datetime = temp_request_date
        self.request_expiration_date: datetime.datetime = temp_expiration_date
        self.correct_for_1_day_requests()

    def correct_for_1_day_requests(self):
        try:
            if self.request_expiration_date.date() <= self.request_date.date():
                if (
                    self.request_expiration_date.hour == 0
                    and self.request_expiration_date.minute == 0
                ):
                    self.request_expiration_date = self.request_expiration_date.replace(
                        hour=23, minute=59
                    )
                if self.request_date.hour == 0 and self.request_date.minute == 0:
                    self.request_date = self.request_date.replace(hour=0, minute=1)
            self.make_request_utc()
        except Exception as ee:
            logging.error(ee)
            self.errors.append(("Time alignment issues", "both dates"))

    def to_dict(self):
        return {
            "requestLevel": "Item",
            "requestType": self.request_type,
            "fulfilmentPreference": self.fulfillment_preference,
            "requester": {"barcode": self.patron_barcode},
            "requesterId": self.patron_id,
            "item": {"barcode": self.item_barcode},
            "itemId": self.item_id,
            "instanceId": self.instance_id,
            "holdingsRecordId": self.holdings_record_id,
            "requestExpirationDate": self.request_expiration_date.isoformat(),
            "patronComments": self.comment,
            "pickupServicePointId": self.pickup_servicepoint_id,
            "requestDate": self.request_date.isoformat(),
            "id": str(uuid.uuid4()),
        }

    def serialize(self):
        req = self.to_dict()
        required = [
            "instanceId",
            "requesterId",
            "requestType",
            "requestLevel",
            "requestDate",
            "holdingsRecordId",
            "itemId",
            "fulfilmentPreference",
            "pickupServicePointId",
        ]
        if req["requestLevel"] == "Title":
            required = [r for r in required if r not in ["itemId", "holdingsRecordId"]]
        missing = [r for r in required if not req.get(r, "")]
        if any(missing):
            raise TransformationRecordFailedError(
                "", "Required properties missing:" ", ".join(missing)
            )
        return req

    def to_source_dict(self):
        return {
            "item_barcode": self.item_barcode,
            "patron_barcode": self.patron_barcode,
            "request_date": self.request_date.isoformat(),
            "request_expiration_date": self.request_expiration_date.isoformat(),
            "comment": self.comment,
            "request_type": self.request_type,
            "pickup_servicepoint_id": self.pickup_servicepoint_id,
        }

    def make_request_utc(self):
        try:
            if self.tenant_timezone != ZoneInfo("UTC"):
                self.request_date = self.request_date.astimezone(ZoneInfo("UTC"))
                self.request_expiration_date = self.request_expiration_date.astimezone(
                    ZoneInfo("UTC")
                )
        except Exception:
            self.errors.append(("UTC correction issues", "both dates"))
