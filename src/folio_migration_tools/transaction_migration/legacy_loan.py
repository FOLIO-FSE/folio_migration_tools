import logging
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from dateutil.parser import parse


class LegacyLoan(object):
    def __init__(self, legacy_loan_dict, utc_difference=0, row=0):
        # validate
        correct_headers = [
            "item_barcode",
            "patron_barcode",
            "due_date",
            "out_date",
            "renewal_count",
            "next_item_status",
        ]
        legal_statuses = [
            "",
            "Aged to lost",
            "Checked out",
            "Claimed returned",
            "Declared lost",
            "Lost and paid",
        ]
        self.utc_difference = utc_difference
        self.errors = []
        for prop in correct_headers:
            if prop not in legacy_loan_dict:
                self.errors.append(("Missing properties in legacy data", prop))
            if prop != "next_item_status" and not legacy_loan_dict[prop].strip():
                self.errors.append(("Empty properties in legacy data", prop))
        try:
            temp_date_due: datetime = parse(legacy_loan_dict["due_date"])
        except Exception as ee:
            logging.error(ee)
            self.errors.append(("Parse date failure. Setting UTC NOW", "due_date"))
            temp_date_due = datetime.now(timezone.utc)
        try:
            temp_date_out: datetime = parse(legacy_loan_dict["out_date"])
        except Exception:
            temp_date_out = datetime.now(timezone.utc)
            self.errors.append(("Parse date failure. Setting UTC NOW", "out_date"))

        # good to go, set properties
        self.item_barcode: str = legacy_loan_dict["item_barcode"].strip()
        self.patron_barcode: str = legacy_loan_dict["patron_barcode"].strip()
        self.due_date: datetime = temp_date_due
        self.out_date: datetime = temp_date_out
        self.make_utc()
        self.correct_for_1_day_loans()
        self.renewal_count = int(legacy_loan_dict["renewal_count"])
        self.next_item_status = legacy_loan_dict.get("next_item_status", "").strip()
        if self.next_item_status not in legal_statuses:
            self.errors.append(("Not an allowed status", self.next_item_status))

    def correct_for_1_day_loans(self):
        try:
            if self.due_date <= self.out_date:
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
            hours_to_add = -1 * self.utc_difference
            if self.utc_difference != 0:
                self.due_date = self.due_date + timedelta(hours=hours_to_add)
                self.out_date = self.out_date + timedelta(hours=hours_to_add)
        except Exception:
            self.errors.append(("UTC correction issues", "both dates"))
