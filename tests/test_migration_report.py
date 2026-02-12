import io
import json
import logging
from datetime import datetime, timezone
from unittest.mock import patch

from dateutil import parser

from folio_migration_tools.migration_report import MigrationReport, as_str


def test_time_diff():
    start = parser.parse("2022-06-29T20:21:22")
    end = parser.parse("2022-06-30T21:22:23")
    nice_diff = str(end - start)
    assert nice_diff == "1 day, 1:01:01"


class TestMigrationReport:
    """Tests for the MigrationReport class."""

    def test_init_creates_empty_report(self):
        """Test that a new MigrationReport has empty report and stats dicts."""
        report = MigrationReport()
        assert report.report == {}
        assert report.stats == {}

    def test_add_creates_new_section(self):
        """Test that add() creates a new section if it doesn't exist."""
        report = MigrationReport()
        report.add("TestSection", "test_measure", 5)

        assert "TestSection" in report.report
        assert report.report["TestSection"]["blurb_id"] == "TestSection"
        assert report.report["TestSection"]["test_measure"] == 5

    def test_add_increments_existing_measure(self):
        """Test that add() increments an existing measure."""
        report = MigrationReport()
        report.add("TestSection", "test_measure", 5)
        report.add("TestSection", "test_measure", 3)

        assert report.report["TestSection"]["test_measure"] == 8

    def test_add_defaults_to_increment_by_one(self):
        """Test that add() defaults to incrementing by 1."""
        report = MigrationReport()
        report.add("TestSection", "test_measure")
        report.add("TestSection", "test_measure")

        assert report.report["TestSection"]["test_measure"] == 2

    def test_add_multiple_measures_same_section(self):
        """Test that multiple measures can be added to the same section."""
        report = MigrationReport()
        report.add("TestSection", "measure_a", 10)
        report.add("TestSection", "measure_b", 20)

        assert report.report["TestSection"]["measure_a"] == 10
        assert report.report["TestSection"]["measure_b"] == 20

    def test_set_creates_new_section(self):
        """Test that set() creates a new section if it doesn't exist."""
        report = MigrationReport()
        report.set("TestSection", "test_measure", 42)

        assert "TestSection" in report.report
        assert report.report["TestSection"]["test_measure"] == 42

    def test_set_overwrites_existing_value(self):
        """Test that set() overwrites rather than increments."""
        report = MigrationReport()
        report.set("TestSection", "test_measure", 10)
        report.set("TestSection", "test_measure", 5)

        assert report.report["TestSection"]["test_measure"] == 5

    def test_add_general_statistics_shortcut(self):
        """Test that add_general_statistics adds to GeneralStatistics section."""
        report = MigrationReport()
        report.add_general_statistics("Records processed")
        report.add_general_statistics("Records processed")

        assert "GeneralStatistics" in report.report
        assert report.report["GeneralStatistics"]["Records processed"] == 2

    def test_write_json_report_empty(self):
        """Test that write_json_report writes valid JSON for empty report."""
        report = MigrationReport()
        output = io.StringIO()

        report.write_json_report(output)

        output.seek(0)
        result = json.load(output)
        assert result == {}

    def test_write_json_report_with_data(self):
        """Test that write_json_report writes all report data as valid JSON."""
        report = MigrationReport()
        report.add("GeneralStatistics", "Records processed", 100)
        report.add("GeneralStatistics", "Records failed", 5)
        report.add("LocationMapping", "Main Library", 50)
        report.add("LocationMapping", "Branch Library", 45)

        output = io.StringIO()
        report.write_json_report(output)

        output.seek(0)
        result = json.load(output)

        assert "GeneralStatistics" in result
        assert result["GeneralStatistics"]["Records processed"] == 100
        assert result["GeneralStatistics"]["Records failed"] == 5
        assert result["GeneralStatistics"]["blurb_id"] == "GeneralStatistics"

        assert "LocationMapping" in result
        assert result["LocationMapping"]["Main Library"] == 50
        assert result["LocationMapping"]["Branch Library"] == 45

    def test_write_json_report_is_indented(self):
        """Test that write_json_report produces indented JSON."""
        report = MigrationReport()
        report.add("TestSection", "measure", 1)

        output = io.StringIO()
        report.write_json_report(output)

        output.seek(0)
        content = output.read()

        # Check for indentation (2 spaces as specified in the code)
        assert "\n  " in content

    def test_write_json_report_preserves_set_values(self):
        """Test that values set with set() are preserved in JSON output."""
        report = MigrationReport()
        report.set("GeneralStatistics", "Total records", 1000)

        output = io.StringIO()
        report.write_json_report(output)

        output.seek(0)
        result = json.load(output)

        assert result["GeneralStatistics"]["Total records"] == 1000

    def test_write_migration_report_writes_header(self):
        """Test that write_migration_report writes the report header and timings."""
        # Mock i18n functions to return predictable values
        with patch(
            "folio_migration_tools.migration_report.i18n.t", side_effect=lambda x, **kw: x
        ), patch("folio_migration_tools.migration_report.i18n_t", side_effect=lambda x: x):
            report = MigrationReport()
            report.add("GeneralStatistics", "Records processed", 100)

            output = io.StringIO()
            start_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            report.write_migration_report("Test Report", output, start_time)

            output.seek(0)
            content = output.read()

            # Check header is present
            assert "# Test Report" in content
            # Check timings section
            assert "Timings" in content
            assert "2024-01-15T10:00:00" in content

    def test_write_migration_report_includes_sections(self):
        """Test that write_migration_report includes all report sections."""
        with patch(
            "folio_migration_tools.migration_report.i18n.t", side_effect=lambda x, **kw: x
        ), patch("folio_migration_tools.migration_report.i18n_t", side_effect=lambda x: x):
            report = MigrationReport()
            report.add("GeneralStatistics", "Records processed", 100)
            report.add("LocationMapping", "Main Library", 50)

            output = io.StringIO()
            start_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            report.write_migration_report("Test Report", output, start_time)

            output.seek(0)
            content = output.read()

            # Check sections are present
            assert "Records processed" in content
            assert "100" in content
            assert "Main Library" in content
            assert "50" in content

    def test_log_me_logs_report_sections(self, caplog):
        """Test that log_me logs all report sections."""
        import logging as logging_mod

        report = MigrationReport()
        report.add("GeneralStatistics", "Records processed", 100)
        report.add("GeneralStatistics", "Records failed", 5)

        # Re-enable propagation temporarily for test capture
        package_logger = logging_mod.getLogger("folio_migration_tools")
        module_logger = logging_mod.getLogger("folio_migration_tools.migration_report")
        orig_pkg_prop = package_logger.propagate
        orig_mod_prop = module_logger.propagate
        package_logger.propagate = True
        module_logger.propagate = True
        try:
            caplog.set_level(logging_mod.INFO)
            report.log_me()

            assert "GeneralStatistics" in caplog.text
            assert "Records processed" in caplog.text
            assert "100" in caplog.text
        finally:
            package_logger.propagate = orig_pkg_prop
            module_logger.propagate = orig_mod_prop


class TestAsStr:
    """Tests for the as_str helper function."""

    def test_as_str_with_string(self):
        """Test as_str with a string input."""
        result = as_str("test")
        assert result == ("test", "")

    def test_as_str_with_number(self):
        """Test as_str with a numeric input."""
        result = as_str(42)
        assert result == ("42", "")

    def test_as_str_with_none(self):
        """Test as_str with None input."""
        result = as_str(None)
        assert result == ("None", "")

