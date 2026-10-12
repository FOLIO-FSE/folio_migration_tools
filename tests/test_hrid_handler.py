from unittest.mock import MagicMock, patch

from pymarc import Field, Record

from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.migration_report import MigrationReport


def test_handle_035_generation_reports_failure_when_035_cannot_be_added():
    record = Record()
    record.add_field(Field(tag="001", data="12345"))
    report = MigrationReport()

    with patch.object(Record, "add_ordered_field", side_effect=ValueError("boom")):
        HRIDHandler.handle_035_generation(record, ["12345"], report, False)

    assert report.report["HridHandling"]["Failed to create 035 from 001"] == 1


def test_store_hrid_settings_puts_with_folio_http_client():
    folio_client = MagicMock()
    folio_client.folio_get_single_object.return_value = {
        "instances": {"prefix": "in", "startNumber": 1},
        "holdings": {"prefix": "ho", "startNumber": 1},
        "items": {"prefix": "it", "startNumber": 1},
        "commonRetainLeadingZeroes": True,
    }
    http_client = folio_client.get_folio_http_client.return_value.__enter__.return_value
    http_client.put.return_value = MagicMock(status_code=204)
    handler = HRIDHandler(folio_client, HridHandling.default, MigrationReport(), False)
    handler.instance_hrid_counter = 42

    handler.store_hrid_settings()

    http_client.put.assert_called_once()
    args, kwargs = http_client.put.call_args
    assert args == ("/hrid-settings-storage/hrid-settings",)
    assert kwargs["json"]["instances"]["startNumber"] == 42
    assert "headers" not in kwargs
    http_client.put.return_value.raise_for_status.assert_called_once()
