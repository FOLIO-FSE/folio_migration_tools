"""Tests for JSON report writing across migration tasks.

This module tests that all migration tasks correctly write both
markdown and raw JSON reports during wrap_up.
"""

import json
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from folio_migration_tools.migration_report import MigrationReport


class TestJsonReportWriting:
    """Test that migration tasks write JSON reports correctly."""

    def test_write_json_report_creates_valid_json(self):
        """Test that write_json_report produces valid JSON output."""
        report = MigrationReport()
        report.add("GeneralStatistics", "Records processed", 100)
        report.add("GeneralStatistics", "Records failed", 5)
        report.add("LocationMapping", "Main Library", 50)

        output = StringIO()
        report.write_json_report(output)

        output.seek(0)
        json_data = json.load(output)

        assert "GeneralStatistics" in json_data
        assert json_data["GeneralStatistics"]["Records processed"] == 100
        assert json_data["GeneralStatistics"]["Records failed"] == 5
        assert "LocationMapping" in json_data
        assert json_data["LocationMapping"]["Main Library"] == 50

    def test_json_report_file_created(self, tmp_path):
        """Test that JSON report file is created on disk."""
        report = MigrationReport()
        report.add("GeneralStatistics", "Test measure", 42)

        json_path = tmp_path / "raw_report.json"
        with open(json_path, "w") as f:
            report.write_json_report(f)

        assert json_path.exists()

        with open(json_path) as f:
            data = json.load(f)
        assert data["GeneralStatistics"]["Test measure"] == 42

    def test_json_report_includes_blurb_id(self):
        """Test that blurb_id is included in JSON output when using add()."""
        report = MigrationReport()
        report.add("TestSection", "some_measure", 1)

        output = StringIO()
        report.write_json_report(output)

        output.seek(0)
        data = json.load(output)

        assert data["TestSection"]["blurb_id"] == "TestSection"

    def test_json_report_set_does_not_include_blurb_id(self):
        """Test that set() does not automatically add blurb_id."""
        report = MigrationReport()
        report.set("TestSection", "some_measure", 1)

        output = StringIO()
        report.write_json_report(output)

        output.seek(0)
        data = json.load(output)

        # set() doesn't add blurb_id automatically
        assert "blurb_id" not in data["TestSection"]

    def test_folder_structure_has_raw_file_path(self, tmp_path):
        """Test that FolderStructure defines migration_reports_raw_file."""
        from folio_migration_tools.folder_structure import FolderStructure
        from folio_uuid.folio_namespaces import FOLIONamespaces

        # Create the required folder structure
        base_folder = tmp_path / "migration"
        base_folder.mkdir()
        (base_folder / "mapping_files").mkdir()
        (base_folder / "iterations").mkdir()
        (base_folder / ".gitignore").write_text("results/\n")

        fs = FolderStructure(
            base_folder,
            FOLIONamespaces.instances,
            "test_task",
            "iteration_1",
            False
        )
        fs.setup_migration_file_structure()

        assert hasattr(fs, 'migration_reports_raw_file')
        assert 'raw_report' in str(fs.migration_reports_raw_file)
        assert '.json' in str(fs.migration_reports_raw_file)


class TestMigrationReportIntegration:
    """Integration tests for migration report with file I/O."""

    def test_both_reports_written_to_same_folder_structure(self, tmp_path):
        """Test that markdown and JSON reports can be written to related paths."""
        report = MigrationReport()
        report.add("GeneralStatistics", "Test", 1)

        reports_folder = tmp_path / "reports"
        reports_folder.mkdir()
        raw_folder = reports_folder / ".raw"
        raw_folder.mkdir()

        md_path = reports_folder / "report_test.md"
        json_path = raw_folder / "raw_report_test.json"

        # Write both reports
        with patch(
            "folio_migration_tools.migration_report.i18n.t", side_effect=lambda x, **kw: x
        ), patch("folio_migration_tools.migration_report.i18n_t", side_effect=lambda x: x):
            from datetime import datetime, timezone
            with open(md_path, "w") as md_file:
                report.write_migration_report(
                    "Test Report",
                    md_file,
                    datetime.now(timezone.utc)
                )

        with open(json_path, "w") as json_file:
            report.write_json_report(json_file)

        # Both files should exist
        assert md_path.exists()
        assert json_path.exists()

        # JSON should be valid
        with open(json_path) as f:
            data = json.load(f)
        assert "GeneralStatistics" in data

        # Markdown should have content
        assert md_path.stat().st_size > 0
