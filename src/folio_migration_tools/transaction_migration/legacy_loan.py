import json
import logging
import i18n
from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil import tz
from dateutil.parser import parse, ParserError

from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.custom_exceptions import TransformationProcessError

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

        self.legacy_loan_dict = legacy_loan_dict
        self.tenant_timezone = tenant_timezone
        self.errors = []
        self.row = row
        for prop in correct_headers:
            if prop not in self.legacy_loan_dict and prop not in optional_headers:
                self.errors.append((f"Missing properties in legacy data {row=}", prop))
            if (
                prop != "next_item_status"
                and not self.legacy_loan_dict.get(prop, "").strip()
                and prop not in optional_headers
            ):
                self.errors.append((f"Empty properties in legacy data {row=}", prop))
        try:
            temp_date_due: datetime = parse(self.legacy_loan_dict["due_date"])
            if temp_date_due.tzinfo != tz.UTC:
                temp_date_due = temp_date_due.replace(tzinfo=self.tenant_timezone)
                self.report(
                    f"Provided due_date is not UTC in {row=}, "
                    f"setting tz-info to tenant timezone ({self.tenant_timezone})"
                )
            if temp_date_due.hour == 0 and temp_date_due.minute == 0:
                temp_date_due = temp_date_due.replace(hour=23, minute=59)
                self.report(
                    f"Hour and minute not specified for due date in {row=}. "
                    "Assuming end of local calendar day (23:59)..."
                )
        except (ParserError, OverflowError) as ee:
            logging.error(ee)
            self.errors.append((f"Parse date failure in {row=}. Setting UTC NOW", "due_date"))
            temp_date_due = datetime.now(ZoneInfo("UTC"))
        try:
            temp_date_out: datetime = parse(self.legacy_loan_dict["out_date"])
            if temp_date_out.tzinfo != tz.UTC:
                temp_date_out = temp_date_out.replace(tzinfo=self.tenant_timezone)
                self.report(
                    f"Provided out_date is not UTC in {row=}, "
                    f"setting tz-info to tenant timezone ({self.tenant_timezone})"
                )
        except (ParserError, OverflowError):
            temp_date_out = datetime.now(
                ZoneInfo("UTC")
            )  # TODO: Consider moving this assignment block above the temp_date_due
            self.errors.append((f"Parse date failure in {row=}. Setting UTC NOW", "out_date"))

        # good to go, set properties
        self.item_barcode: str = self.legacy_loan_dict["item_barcode"].strip()
        self.patron_barcode: str = self.legacy_loan_dict["patron_barcode"].strip()
        self.proxy_patron_barcode: str = self.legacy_loan_dict.get("proxy_patron_barcode", "")
        self.due_date: datetime = temp_date_due
        self.out_date: datetime = temp_date_out
        self.correct_for_1_day_loans()
        self.make_utc()
        self.renewal_count = self.set_renewal_count(self.legacy_loan_dict)
        self.next_item_status = self.legacy_loan_dict.get("next_item_status", "").strip()
        if self.next_item_status not in legal_statuses:
            self.errors.append((f"Not an allowed status {row=}", self.next_item_status))
        self.service_point_id = (
            self.legacy_loan_dict["service_point_id"]
            if self.legacy_loan_dict.get("service_point_id", "")
            else fallback_service_point_id
        )

    def set_renewal_count(self, loan: dict) -> int:
        if "renewal_count" in loan:
            renewal_count = loan["renewal_count"]
            try:
                return int(renewal_count)
            except ValueError:
                self.report(f"Unresolvable {renewal_count=} was replaced with 0.")
        else:
            self.report(f"Missing renewal count was replaced with 0.")
        return 0

    def correct_for_1_day_loans(self):
        if self.due_date.date() <= self.out_date.date():
            if self.due_date.hour == 0:
                self.due_date = self.due_date.replace(hour=23, minute=59)
            if self.out_date.hour == 0:
                self.out_date = self.out_date.replace(hour=0, minute=1)
        if self.due_date <= self.out_date:
            raise TransformationProcessError(self.row, i18n.t("Due date is before out date, or date information is missing from both"), json.dumps(self.legacy_loan_dict, indent=2))

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
        except TypeError:
            self.errors.append((f"UTC correction issues {self.row}", "both dates"))

    def report(self, what_to_report: str):
        self.migration_report.add("Details", what_to_report)
