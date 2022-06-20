import uuid
from zoneinfo import ZoneInfo

import pytest

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest


def test_serialize_fails():
    with pytest.raises(TransformationRecordFailedError):
        legacy_request_dict = {
            "item_barcode": "ib",
            "patron_barcode": "pb",
            "request_date": "2022-09-01",
            "request_expiration_date": "2022-10-01",
            "comment": "comment",
            "request_type": "Hold",
            "pickup_servicepoint_id": str(uuid.uuid4()),
        }
        LegacyRequest(legacy_request_dict).serialize(FolioRelease.lotus)


def test_serialize_success_lotus():
    legacy_request_dict = {
        "item_barcode": "ib",
        "patron_barcode": "pb",
        "request_date": "2022-09-01",
        "request_expiration_date": "2022-10-01",
        "comment": "comment",
        "request_type": "Hold",
        "pickup_servicepoint_id": str(uuid.uuid4()),
    }
    legacy_request = LegacyRequest(legacy_request_dict)
    legacy_request.holdings_record_id = str(uuid.uuid4())
    legacy_request.item_id = str(uuid.uuid4())
    legacy_request.instance_id = str(uuid.uuid4())
    legacy_request.patron_id = str(uuid.uuid4())
    assert legacy_request.serialize(FolioRelease.lotus)


def test_serialize_success_kiwi():
    legacy_request_dict = {
        "item_barcode": "ib",
        "patron_barcode": "pb",
        "request_date": "2022-09-01",
        "request_expiration_date": "2022-10-01",
        "comment": "comment",
        "request_type": "Hold",
        "pickup_servicepoint_id": str(uuid.uuid4()),
    }
    legacy_request = LegacyRequest(legacy_request_dict)
    legacy_request.item_id = str(uuid.uuid4())
    legacy_request.patron_id = str(uuid.uuid4())
    assert legacy_request.serialize(FolioRelease.kiwi)


def test_request_dates_to_utc():
    legacy_request_dict = {
        "item_barcode": "ib",
        "patron_barcode": "pb",
        "request_date": "2022-09-01",
        "request_expiration_date": "2022-10-01",
        "comment": "comment",
        "request_type": "Hold",
        "pickup_servicepoint_id": str(uuid.uuid4()),
    }
    legacy_request = LegacyRequest(legacy_request_dict, ZoneInfo("America/Chicago"))
    assert legacy_request.request_date.isoformat() == "2022-09-01T05:00:00+00:00"
    assert legacy_request.request_expiration_date.isoformat() == "2022-10-02T04:59:00+00:00"
