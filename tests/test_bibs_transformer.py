from pymarc import MARCReader
from pymarc import Record

from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.report_blurbs import Blurbs


def test_set_leader():
    migration_report = MigrationReport()
    path = "./tests/test_data/corrupt_leader.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        BibsTransformer.set_leader(record, migration_report)
        assert record.leader.endswith("4500")
        assert record.leader[9] == "a"
        assert record.leader[10] == "2"
        assert record.leader[11] == "2"
        vals = migration_report.report[Blurbs.LeaderManipulation[0]].items()
        # Should be 4?
        assert len(vals) == 5
