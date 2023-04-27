import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import dateutil

from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs

utc = ZoneInfo("UTC")


class LegacyManualFeeFine(object):
    def __init__(
        self,
        legacy_feesfines_dict,
        fallback_fee_fine_owner: str,
        fallback_fee_fine_type: str,
        migration_report: MigrationReport,
        tenant_timezone=utc,
    ):
        self.migration_report: MigrationReport = migration_report
        # validate
        self.expected_headers = [
            "patron_barcode",
            "item_barcode",
            "billed_date",
            "total_amount",
            "remaining_amount",
            "fee_fine_owner",
            "fee_fine_type",
        ]

        optional_headers = ["fee_fine_owner", "fee_fine_owner", "remaining_amount"]

        self.tenant_timezone = tenant_timezone
        self.errors = []

        for header in self.expected_headers:
            if header not in legacy_feesfines_dict and header not in optional_headers:
                self.errors.append(("Missing properties in legacy data", header))

        self.item_barcode: str = legacy_feesfines_dict.get("item_barcode").strip()
        self.patron_barcode: str = legacy_feesfines_dict.get("patron_barcode").strip()
        self.billed_date: datetime = self.parse_date(legacy_feesfines_dict["billed_date"])
        self.total_amount: float = float(legacy_feesfines_dict.get("totalt_amount"))
        self.remaining_amount: float = float(
            legacy_feesfines_dict.get(
                "remaining_amount", legacy_feesfines_dict.get("total_amount")
            )
        )
        self.fee_fine_owner: str = legacy_feesfines_dict.get(
            "fee_fine_owner", fallback_fee_fine_owner
        )
        self.fee_fine_type: str = legacy_feesfines_dict.get(
            "fee_fine_type", fallback_fee_fine_type
        )

    # TODO Understand what this is used for!
    def to_dict(self):
        return {
            "item_barcode": self.item_barcode,
            "patron_barcode": self.patron_barcode,
            "billed_date": self.billed_date,
            "total_amount": self.total_amount,
            "remaining_amount": self.remaining_amount,
            "fee_fine_owner": self.fee_fine_owner,
            "fee_fine_type": self.fee_fine_type,
        }

    def parse_date(self, billed_date):
        try:
            billed_date: datetime = dateutil.parse(billed_date)
            if billed_date.tzinfo != dateutil.tz.UTC:
                billed_date = billed_date.replace(tzinfo=self.tenant_timezone)
                self.report(
                    f"Provided billed_date is not UTC. "
                    f"Using tenant time zone ({self.tenant_timezone})"
                )
            if billed_date.hour == 0 and billed_date.minute == 0:
                billed_date = billed_date.replace(hour=23, minute=59)
                self.report(
                    "Hour and minute not specified. " "Setting to end of calendar day (23:59)."
                )
        except Exception as ee:
            logging.error(ee)
            self.errors.append(
                ("Failed to parse billed_date {billed_date}. " "Setting to UTC NOW", "billed_date")
            )
            billed_date: datetime = datetime.now(ZoneInfo("UTC"))

    def report(self, what_to_report: str):
        self.migration_report.add(Blurbs.Details, what_to_report)
