import uuid
from typing import Dict
from typing import List
from typing import Tuple

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError


class LegacyReserve(object):
    def __init__(self, legacy_request_dict: Dict, folio_client: FolioClient, row: int = 0):
        # validate
        correct_headers = ["legacy_identifier", "item_barcode"]
        for h in correct_headers:
            if h not in legacy_request_dict:
                raise TransformationProcessError(
                    int,
                    "Missing header in file. The following are required:",
                    ",".join(correct_headers),
                )
        self.errors: List[Tuple[str, str]] = [
            ("Missing properties in legacy data", prop)
            for prop in correct_headers
            if prop not in legacy_request_dict
        ]
        self.id = str(uuid.uuid4())
        self.item_barcode: str = legacy_request_dict["item_barcode"].strip()
        if not self.item_barcode:
            self.errors.append(("Missing data.", "item_barcode"))
        self.legacy_identifier: str = legacy_request_dict["legacy_identifier"].strip()
        if not self.legacy_identifier:
            self.errors.append(("Missing data.", "legacy_identifier"))
        self.course_listing_id: str = str(
            FolioUUID(
                folio_client.okapi_url,
                FOLIONamespaces.course_listing,
                legacy_request_dict["legacy_identifier"],
            )
        )

    def to_dict(self):
        return {
            "courseListingId": self.course_listing_id,
            "copiedItem": {"barcode": self.item_barcode},
            "id": self.id,
        }
