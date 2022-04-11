import datetime
import uuid
from dateutil.parser import parse


class LegacyRequest(object):
    def __init__(self, legacy_request_dict, row=0):
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
        self.item_id = ""
        self.patron_barcode = legacy_request_dict["patron_barcode"].strip()
        self.comment = legacy_request_dict["comment"].strip()
        self.request_type = legacy_request_dict["request_type"].strip()
        self.pickup_servicepoint_id = legacy_request_dict[
            "pickup_servicepoint_id"
        ].strip()
        self.fulfillment_preference = "Hold Shelf"

        try:
            temp_request_date: datetime = parse(legacy_request_dict["request_date"])
        except Exception as ee:
            self.errors.append(("Parse date failure. Setting UTC NOW", "request_date"))
            temp_request_date = datetime.utcnow()
        try:
            temp_expiration_date: datetime = parse(
                legacy_request_dict["request_expiration_date"]
            )
        except Exception as ee:
            temp_expiration_date = datetime.utcnow()
            self.errors.append(
                ("Parse date failure. Setting UTC NOW", "request_expiration_date")
            )

        self.request_date = temp_request_date
        self.request_expiration_date = temp_expiration_date

    def to_dict(self):
        return {
            "requestType": self.request_type,
            "fulfilmentPreference": self.fulfillment_preference,
            "requester": {"barcode": self.patron_barcode},
            "requesterId": self.patron_id,
            "item": {"barcode": self.item_barcode},
            "itemId": self.item_id,
            "requestExpirationDate": self.request_expiration_date.isoformat(),
            "patronComments": self.comment,
            "pickupServicePointId": self.pickup_servicepoint_id,
            "requestDate": self.request_date.isoformat(),
            "id": str(uuid.uuid4()),
        }
