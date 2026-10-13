import uuid
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from folio_migration_tools.circulation_helper import CirculationHelper
from folio_migration_tools.migration_report import MigrationReport
from .test_infrastructure import mocked_classes


def test_init():
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    assert circ_helper.folio_client
    assert circ_helper.service_point_id == sp_id
    assert not any(circ_helper.missing_patron_barcodes)
    assert not any(circ_helper.missing_item_barcodes)
    assert not any(circ_helper.migration_report.report)


def test_get_user_by_barcode_already_missing(caplog):
    """Test that get_user_by_barcode logs when user barcode is already in missing set."""
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    # Add a barcode to the missing set first
    circ_helper.missing_patron_barcodes.add("already_missing_bc")

    with caplog.at_level("INFO"):
        result = circ_helper.get_user_by_barcode("already_missing_bc")

    assert result == {}
    assert "User is already detected as missing" in caplog.text


def test_get_user_by_barcode_api_error(caplog):
    """Test that get_user_by_barcode logs and returns empty dict on API error."""
    mocked_folio = mocked_classes.mocked_folio_client()
    mocked_folio.folio_get = MagicMock(side_effect=Exception("API Connection Error"))
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    with caplog.at_level("ERROR"):
        result = circ_helper.get_user_by_barcode("test_barcode")

    assert result == {}
    assert "API Connection Error" in caplog.text


def test_get_item_by_barcode_already_missing(caplog):
    """Test that get_item_by_barcode logs when item barcode is already in missing set."""
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    # Add a barcode to the missing set first
    circ_helper.missing_item_barcodes.add("already_missing_item_bc")

    with caplog.at_level("INFO"):
        result = circ_helper.get_item_by_barcode("already_missing_item_bc")

    assert result == {}
    assert "Item is already detected as missing" in caplog.text


def test_get_item_by_barcode_api_error(caplog):
    """Test that get_item_by_barcode logs and returns empty dict on API error."""
    mocked_folio = mocked_classes.mocked_folio_client()
    mocked_folio.folio_get = MagicMock(side_effect=Exception("Item API Error"))
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    with caplog.at_level("ERROR"):
        result = circ_helper.get_item_by_barcode("test_item_barcode")

    assert result == {}
    assert "Item API Error" in caplog.text


def test_get_active_loan_by_item_id_exception(caplog):
    """Test that get_active_loan_by_item_id logs error and returns empty dict on exception."""
    mocked_folio = mocked_classes.mocked_folio_client()
    mocked_folio.folio_get = MagicMock(side_effect=Exception("Loan lookup failed"))
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    with caplog.at_level("ERROR"):
        result = circ_helper.get_active_loan_by_item_id("item-uuid-123")

    assert result == {}
    assert "Loan lookup failed" in caplog.text


def test_get_holding_by_uuid_exception(caplog):
    """Test that get_holding_by_uuid logs error and returns empty dict on exception."""
    mocked_folio = mocked_classes.mocked_folio_client()
    mocked_folio.folio_get_single_object = MagicMock(side_effect=Exception("Holdings lookup failed"))
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    with caplog.at_level("ERROR"):
        result = circ_helper.get_holding_by_uuid("holdings-uuid-456")

    assert result == {}
    assert "Holdings lookup failed" in caplog.text


def test_check_out_by_barcode_handles_internal_server_error():
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())

    class DummyInternalServerError(Exception):
        pass

    legacy_loan = MagicMock()
    legacy_loan.item_barcode = "item123"
    legacy_loan.patron_barcode = "user123"
    legacy_loan.proxy_patron_barcode = ""
    legacy_loan.service_point_id = sp_id
    legacy_loan.out_date = datetime.now(timezone.utc)
    legacy_loan.due_date = datetime.now(timezone.utc)

    mocked_folio.folio_post = MagicMock(
        side_effect=DummyInternalServerError("FOLIO internal server error")
    )

    with patch(
        "folio_migration_tools.circulation_helper.FolioInternalServerError",
        DummyInternalServerError,
    ):
        result = circ_helper.check_out_by_barcode(legacy_loan)

    assert result.was_successful is False
    assert result.should_be_retried is False
    assert "internal server error" in result.error_message.lower()


def _checkout_loan(sp_id):
    legacy_loan = MagicMock()
    legacy_loan.item_barcode = "item123"
    legacy_loan.patron_barcode = "user123"
    legacy_loan.proxy_patron_barcode = ""
    legacy_loan.service_point_id = sp_id
    legacy_loan.out_date = datetime.now(timezone.utc)
    legacy_loan.due_date = datetime.now(timezone.utc)
    legacy_loan.has_inverted_dates = False
    return legacy_loan


def test_check_out_by_barcode_sends_legacy_dates():
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    legacy_loan = _checkout_loan(sp_id)
    legacy_loan.out_date = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    legacy_loan.due_date = datetime(2024, 1, 15, 23, 59, tzinfo=timezone.utc)
    mocked_folio.folio_post = MagicMock(return_value={"id": "l1"})

    circ_helper.check_out_by_barcode(legacy_loan)

    payload = mocked_folio.folio_post.call_args[0][1]
    assert payload["loanDate"] == "2024-01-01T00:01:00+00:00"
    assert payload["overrideBlocks"]["itemNotLoanableBlock"]["dueDate"] == (
        "2024-01-15T23:59:00+00:00"
    )


def test_check_out_by_barcode_sends_placeholder_due_date_for_inverted_dates():
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    legacy_loan = _checkout_loan(sp_id)
    legacy_loan.out_date = datetime(2024, 1, 10, 0, 1, tzinfo=timezone.utc)
    legacy_loan.due_date = datetime(2024, 1, 5, 23, 59, tzinfo=timezone.utc)
    legacy_loan.has_inverted_dates = True
    mocked_folio.folio_post = MagicMock(return_value={"id": "l1"})

    circ_helper.check_out_by_barcode(legacy_loan)

    payload = mocked_folio.folio_post.call_args[0][1]
    assert payload["loanDate"] == "2024-01-10T00:01:00+00:00"
    assert payload["overrideBlocks"]["itemNotLoanableBlock"]["dueDate"] == (
        "2024-01-11T00:01:00+00:00"
    )


def test_check_out_by_barcode_client_error_reports_actual_status():
    import httpx
    from folioclient.exceptions import FolioClientError

    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    request = httpx.Request("POST", "http://folio/circulation/check-out-by-barcode")
    response = httpx.Response(404, text="Not found", request=request)
    mocked_folio.folio_post = MagicMock(
        side_effect=FolioClientError("Not found", request=request, response=response)
    )

    result = circ_helper.check_out_by_barcode(_checkout_loan(sp_id))

    assert result.was_successful is False
    assert result.should_be_retried is False
    assert result.error_message == "HTTP 404"


def test_check_out_by_barcode_connection_error(caplog):
    import httpx
    from folioclient.exceptions import FolioConnectionError

    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    request = httpx.Request("POST", "http://folio/circulation/check-out-by-barcode")
    mocked_folio.folio_post = MagicMock(side_effect=FolioConnectionError("boom", request=request))

    result = circ_helper.check_out_by_barcode(_checkout_loan(sp_id))

    assert result.was_successful is False
    assert result.should_be_retried is False
    assert result.error_message == "Connection error"
    assert "boom" in caplog.text


def test_extend_open_loan_puts_with_folio_http_client():
    folio_client = MagicMock()
    http_client = folio_client.get_folio_http_client.return_value.__enter__.return_value
    http_client.put.return_value = MagicMock(status_code=204)
    loan = {"id": "loan-1", "metadata": {"createdDate": "x"}}
    due = datetime(2026, 1, 2, tzinfo=timezone.utc)
    out = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert CirculationHelper.extend_open_loan(folio_client, loan, due, out) is True

    args, kwargs = http_client.put.call_args
    assert args == ("/circulation/loans/loan-1",)
    assert kwargs["json"] == {
        "id": "loan-1",
        "dueDate": due.isoformat(),
        "loanDate": out.isoformat(),
    }
    assert kwargs["timeout"] is None
    assert "headers" not in kwargs
    assert "metadata" in loan
