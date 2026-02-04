"""Legacy course reserve data model and validation.

Defines the LegacyReserve class for representing course reserves from legacy ILS systems.
Handles validation, course listing lookups, and transformation to FOLIO course reserve
format. Links items to course listings via barcode lookups.
"""

import uuid
from typing import Dict, List, Tuple

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError


class LegacyReserve(object):
    def __init__(self, legacy_request_dict: Dict, folio_client: FolioClient, row: int = 0):
        """Initialize LegacyReserve from legacy reserve data.

        Args:
            legacy_request_dict (Dict): Dictionary containing legacy reserve data.
            folio_client (FolioClient): FOLIO API client for lookups.
            row (int): Row number in source data for error reporting.
        """
        # validate
        correct_headers = ["legacy_identifier", "item_barcode"]
        for h in correct_headers:
            if h not in legacy_request_dict:
                raise TransformationProcessError(
                    row,
                    "Missing header in file. The following are required:",
                    ", ".join(correct_headers),
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
                folio_client.gateway_url,
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
