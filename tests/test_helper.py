import io
import logging

from folio_migration_tools.helper import Helper

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_log_dats_issue(caplog):
    caplog.set_level(26)
    Helper.log_data_issue("test id", "test log message", "legacy value")
    assert "test log message" in caplog.text
    assert "test id" in caplog.text
    assert "legacy value" in caplog.text


def test_print_mapping_report():
    migration_report_file = io.StringIO()
    mapped_folio_fields = {}
    mapped_legacy_fields = {
        "name": [1, 1],
        "code": [1, 1],
        "org_note": [1, 1],
        "status": [1, 1],
        "organization_types": [1, 1],
        "Alternative Names": [1, 1],
        "Not mapped": [5, 5],
        "interface_1_name": [2, 2],
        "account_status": [1, 1],
        "tgs": [1, 1],
    }

    Helper.print_mapping_report(
        migration_report_file, 5, mapped_folio_fields, mapped_legacy_fields
    )

    report_content = migration_report_file.getvalue()
    
    assert "interface_1_name" in report_content