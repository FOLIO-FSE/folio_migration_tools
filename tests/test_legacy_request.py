import uuid
from zoneinfo import ZoneInfo

import pytest

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
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
        LegacyRequest(legacy_request_dict).serialize()


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
    assert legacy_request.serialize()


def test_post_lotus_serialize_ilr():
    tenant_timezone = ZoneInfo("UTC")
    legacy_request_dict = {
        "item_barcode": "6086090202",
        "patron_barcode": "123456789",
        "pickup_servicepoint_id": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
        "request_date": "2021-11-24 18:24:47-05",
        "request_expiration_date": "2025-08-31 04:00:00-04",
        "comment": "Migrated from previous system",
        "request_type": "Hold",
    }
    legacy_request = LegacyRequest(legacy_request_dict, tenant_timezone, 1)
    legacy_request.patron_id = "patron_id"
    legacy_request.holdings_record_id = "holdings record id"
    legacy_request.instance_id = "instance id"
    legacy_request.item_id = ("itemId",)
    legacy_request.fulfillment_preference = ("fulfilmentPreference",)
    serialized = legacy_request.serialize()
    assert "requestLevel" in serialized


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


def test_correct_for_1_day_requests_exception(caplog):
    """Test that correct_for_1_day_requests logs errors when UTC conversion fails."""
    from unittest.mock import patch

    legacy_request_dict = {
        "item_barcode": "ib",
        "patron_barcode": "pb",
        "request_date": "2022-09-01",
        "request_expiration_date": "2022-09-01",  # Same day to trigger the correction path
        "comment": "comment",
        "request_type": "Hold",
        "pickup_servicepoint_id": str(uuid.uuid4()),
    }
    # First create the request without the patch to get the dates set up
    legacy_request = LegacyRequest(legacy_request_dict, ZoneInfo("America/Chicago"))

    # Now trigger the error path by making make_request_utc raise an exception
    with caplog.at_level("ERROR"):
        with patch.object(
            legacy_request, "make_request_utc", side_effect=Exception("UTC conversion error")
        ):
            legacy_request.correct_for_1_day_requests()

    assert "UTC conversion error" in caplog.text
    assert ("Time alignment issues", "both dates") in legacy_request.errors
