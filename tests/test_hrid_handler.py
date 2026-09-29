from unittest.mock import patch

from pymarc import Field, Record

from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.migration_report import MigrationReport


def test_handle_035_generation_reports_failure_when_035_cannot_be_added():
    record = Record()
    record.add_field(Field(tag="001", data="12345"))
    report = MigrationReport()

    with patch.object(Record, "add_ordered_field", side_effect=ValueError("boom")):
        HRIDHandler.handle_035_generation(record, ["12345"], report, False)

    assert report.report["HridHandling"]["Failed to create 035 from 001"] == 1
