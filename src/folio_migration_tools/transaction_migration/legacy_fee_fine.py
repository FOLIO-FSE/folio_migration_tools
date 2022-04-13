from datetime import datetime
import logging
from datetime import timezone
from dateutil.parser import parse


class LegacyFeeFine(object):
    def __init__(self, legacy_fee_fine_dict, row=0):
        # validate
        correct_headers = [
            "item_barcode",
            "patron_barcode",
            "create_date",
            "remaining",
            "amount",
        ]
        self.errors = []
        for prop in correct_headers:
            if prop not in legacy_fee_fine_dict:
                self.errors.append(("Missing properties in legacy data", prop))
            elif not legacy_fee_fine_dict[prop]:
                self.errors.append(("Empty properties in legacy data", prop))
        try:
            temp_created_date: datetime = parse(legacy_fee_fine_dict["create_date"])
        except Exception as ee:
            logging.error(ee)
            self.errors.append(("Dates that failed to parse", prop))
            temp_created_date = datetime.now(timezone.utc)

        # good to go, set properties
        self.item_barcode = legacy_fee_fine_dict.get("item_barcode", "")
        self.patron_barcode = legacy_fee_fine_dict.get("patron_barcode", "")
        self.created_date = temp_created_date
        self.remaining = legacy_fee_fine_dict.get("remaining", "")
        self.amount = legacy_fee_fine_dict.get("amount", "")
        self.source_dict = legacy_fee_fine_dict
