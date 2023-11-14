import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil import tz
from dateutil.parser import parse

from folio_migration_tools.migration_report import MigrationReport

utc = ZoneInfo("UTC")


class LegacyLoan(object):
    def __init__(
        self,
        legacy_loan_dict,
        fallback_service_point_id: str,
        migration_report: MigrationReport,
        tenant_timezone=utc,
        row=0,
    ):
        self.migration_report: MigrationReport = migration_report
        # validate
        correct_headers = [
            "item_barcode",
            "patron_barcode",
            "due_date",
            "out_date",
            "renewal_count",
            "next_item_status",
            "service_point_id",
        ]
        optional_headers = ["service_point_id", "proxy_patron_barcode"]
        legal_statuses = [
            "",
            "Aged to lost",
            "Checked out",
            "Claimed returned",
            "Declared lost",
            "Lost and paid",
        ]

        self.tenant_timezone = tenant_timezone
        self.errors = []
        for prop in correct_headers:
            if prop not in legacy_loan_dict and prop not in optional_headers:
                self.errors.append(("Missing properties in legacy data", prop))
            if (
                prop != "next_item_status"
                and not legacy_loan_dict.get(prop, "").strip()
                and prop not in optional_headers
            ):
                self.errors.append(("Empty properties in legacy data", prop))
        try:
            temp_date_due: datetime = parse(legacy_loan_dict["due_date"])
            if temp_date_due.tzinfo != tz.UTC:
                temp_date_due = temp_date_due.replace(tzinfo=self.tenant_timezone)
                self.report(
                    f"Provided due_date is not UTC, "
                    f"setting tzinfo to tenant timezone ({self.tenant_timezone})"
                )
            if temp_date_due.hour == 0 and temp_date_due.minute == 0:
                temp_date_due = temp_date_due.replace(hour=23, minute=59)
                self.report(
                    "Hour and minute not specified for due date. "
                    "Assuming end of local calendar day (23:59)..."
                )
        except Exception as ee:
            logging.error(ee)
            self.errors.append(("Parse date failure. Setting UTC NOW", "due_date"))
            temp_date_due = datetime.now(ZoneInfo("UTC"))
        try:
            temp_date_out: datetime = parse(legacy_loan_dict["out_date"])
            if temp_date_out.tzinfo != tz.UTC:
                temp_date_out = temp_date_out.replace(tzinfo=self.tenant_timezone)
                self.report(
                    f"Provided out_date is not UTC, "
                    f"setting tzinfo to tenant timezone ({self.tenant_timezone})"
                )
        except Exception:
            temp_date_out = datetime.now(
                ZoneInfo("UTC")
            )  # TODO: Consider moving this assignment block above the temp_date_due
            self.errors.append(("Parse date failure. Setting UTC NOW", "out_date"))

        # good to go, set properties
        self.item_barcode: str = legacy_loan_dict["item_barcode"].strip()
        self.patron_barcode: str = legacy_loan_dict["patron_barcode"].strip()
        self.proxy_patron_barcode: str = legacy_loan_dict.get("proxy_patron_barcode", "")
        self.due_date: datetime = temp_date_due
        self.out_date: datetime = temp_date_out
        self.correct_for_1_day_loans()
        self.make_utc()
        self.renewal_count = int(legacy_loan_dict["renewal_count"])
        self.next_item_status = legacy_loan_dict.get("next_item_status", "").strip()
        if self.next_item_status not in legal_statuses:
            self.errors.append(("Not an allowed status", self.next_item_status))
        self.service_point_id = (
            legacy_loan_dict["service_point_id"]
            if legacy_loan_dict.get("service_point_id", "")
            else fallback_service_point_id
        )

    def correct_for_1_day_loans(self):
        try:
            if self.due_date.date() <= self.out_date.date():
                if self.due_date.hour == 0:
                    self.due_date = self.due_date.replace(hour=23, minute=59)
                if self.out_date.hour == 0:
                    self.out_date = self.out_date.replace(hour=0, minute=1)
            if self.due_date <= self.out_date:
                raise ValueError("Due date is before out date")
        except Exception:
            self.errors.append(("Time alignment issues", "both dates"))

    def to_dict(self):
        return {
            "item_barcode": self.item_barcode,
            "patron_barcode": self.patron_barcode,
            "due_date": self.due_date.isoformat(),
            "out_date": self.out_date.isoformat(),
            "renewal_count": self.renewal_count,
            "next_item_status": self.next_item_status,
        }

    def make_utc(self):
        try:
            if self.tenant_timezone != ZoneInfo("UTC"):
                self.due_date = self.due_date.astimezone(ZoneInfo("UTC"))
                self.out_date = self.out_date.astimezone(ZoneInfo("UTC"))
        except Exception:
            self.errors.append(("UTC correction issues", "both dates"))

    def report(self, what_to_report: str):
        self.migration_report.add("Details", what_to_report)
