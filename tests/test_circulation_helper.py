import uuid
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
