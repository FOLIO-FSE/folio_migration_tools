"""Legacy request data model and validation.

Defines the LegacyRequest class for representing patron requests from legacy ILS systems.
Handles validation, timezone conversion, request type mapping, and transformation to FOLIO
request format. Supports hold queue positioning and expiration dates.
"""

import datetime
import logging
import uuid
from zoneinfo import ZoneInfo

from dateutil import tz
from dateutil.parser import parse

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.utils import is_uuid

logger = logging.getLogger(__name__)

utc = ZoneInfo("UTC")


class LegacyRequest(object):
    def __init__(
        self,
        legacy_request_dict,
        tenant_timezone=utc,
        row=0,
        service_point_mapping: RefDataMapping | None = None,
        fallback_service_point_id="",
    ):
        """Initialize LegacyRequest from legacy request data.

        Args:
            legacy_request_dict: Dictionary containing legacy request data.
            tenant_timezone: Timezone of the tenant (default: UTC).
            row (int): Row number in source data for error reporting.
            service_point_mapping: Optional RefDataMapping for service point code resolution.
            fallback_service_point_id: Fallback service point ID to use if not in source data.
        """
        # validate
        correct_headers = [
            "item_barcode",
            "patron_barcode",
            "request_date",
            "request_expiration_date",
            "comment",
            "request_type",
        ]
        self.errors = []
        self.row = row

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
        self.pickup_servicepoint_id = self._get_service_point_value(
            legacy_request_dict, fallback_service_point_id, service_point_mapping
        )
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

    def _get_service_point_value(
        self,
        legacy_request_dict,
        fallback_service_point_id,
        service_point_mapping: RefDataMapping | None,
    ):
        """Resolve service point ID from source data or mapping.

        Args:
            legacy_request_dict: Dictionary containing legacy request data.
            fallback_service_point_id: Fallback service point ID to use if not in source data.
            service_point_mapping: Optional RefDataMapping for service point code resolution.

        Returns:
            str: Resolved service point ID.
        """
        raw_value = legacy_request_dict.get("pickup_servicepoint_id", "").strip()

        # Empty value → use fallback
        if not raw_value:
            raw_value = fallback_service_point_id

        # UUID value → pass through unchanged (will be validated later)
        if is_uuid(raw_value):
            return raw_value

        # Code value with mapping
        if service_point_mapping:
            try:
                mapping = service_point_mapping.get_ref_data_mapping(
                    {"service_point_id": raw_value}
                )
                if mapping and "folio_id" in mapping:
                    return mapping["folio_id"]
                else:
                    self.errors.append(
                        (
                            f"Service point code not found in mapping in row {self.row}",
                            raw_value,
                        )
                    )
                    return ""
            except Exception as e:
                logger.warning(
                    f"Error resolving service point '{raw_value}' in row {self.row}: {e}"
                )
                self.errors.append(
                    (
                        f"Error resolving service point code in row {self.row}",
                        raw_value,
                    )
                )
                return ""

        # Code value without mapping → return as-is (will be validated later)
        return raw_value

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
            logger.exception(ee)
            self.errors.append(("Time alignment issues", "both dates"))

    def to_dict(self):
        return {
            "requestLevel": "Item",
            "requestType": self.request_type,
            "fulfillmentPreference": self.fulfillment_preference,
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
            "fulfillmentPreference",
            "pickupServicePointId",
        ]
        if req["requestLevel"] == "Title":
            required = [r for r in required if r not in ["itemId", "holdingsRecordId"]]
        missing = [r for r in required if not req.get(r, "")]
        if any(missing):
            raise TransformationRecordFailedError(
                "", "Required properties missing:, ".join(missing)
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
