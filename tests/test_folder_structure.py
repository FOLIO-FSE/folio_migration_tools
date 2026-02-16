from pathlib import Path
from unittest.mock import patch

import pytest

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.folder_structure import FolderStructure


def fake_verify_folder(self, path: Path):
    # Pretend the folders are all there...
    return None


@patch.object(FolderStructure, "verify_folder", fake_verify_folder)
def test_init():
    folder_structure = FolderStructure(
        "", FOLIONamespaces.other, "test_task", "test_iteration", False
    )
    assert str(folder_structure.base_folder) == "."
    assert str(folder_structure.data_folder) == "iterations/test_iteration/source_data"
    assert str(folder_structure.iteration_folder) == "iterations/test_iteration"
    assert str(folder_structure.iteration_identifier) == "test_iteration"
    assert str(folder_structure.migration_task_name) == "test_task"
    assert str(folder_structure.reports_folder) == "iterations/test_iteration/reports"
    assert str(folder_structure.results_folder) == "iterations/test_iteration/results"


@patch.object(FolderStructure, "verify_folder", fake_verify_folder)
def test_setup_migration_file_structure():
    folder_structure = FolderStructure(
        "", FOLIONamespaces.other, "test_task", "test_iteration", False
    )
    folder_structure.setup_migration_file_structure()

    # Asserts
    assert (
        str(folder_structure.created_objects_path)
        == "iterations/test_iteration/results/folio_other_test_task.json"
    )
    assert (
        str(folder_structure.data_issue_file_path)
        == "iterations/test_iteration/reports/data_issues_log_test_task.tsv"
    )

    assert (
        str(folder_structure.failed_marc_recs_file)
        == "iterations/test_iteration/results/failed_records_test_task.mrc"
    )

    assert str(folder_structure.failed_recs_path).startswith(
        "iterations/test_iteration/results/failed_records_test_task_"
    )
    assert str(folder_structure.failed_recs_path).endswith(".txt")
    assert folder_structure.time_str in str(folder_structure.failed_recs_path)
    assert (
        str(folder_structure.holdings_id_map_path)
        == "iterations/test_iteration/results/holdings_id_map.json"
    )

    assert (
        str(folder_structure.instance_id_map_path)
        == "iterations/test_iteration/results/instances_id_map.json"
    )

    assert str(folder_structure.item_statuses_map_path) == "mapping_files/item_statuses.tsv"

    assert str(folder_structure.loan_type_map_path) == "mapping_files/loan_types.tsv"

    assert str(folder_structure.legacy_records_folder) == "iterations/test_iteration/source_data"

    assert str(folder_structure.mapping_files_folder) == "mapping_files"

    assert (
        str(folder_structure.migration_reports_file)
        == "iterations/test_iteration/reports/report_test_task.md"
    )

    assert str(folder_structure.material_type_map_path) == "mapping_files/material_types.tsv"

    assert str(folder_structure.reports_folder) == "iterations/test_iteration/reports"

    assert str(folder_structure.results_folder) == "iterations/test_iteration/results"

    assert (
        str(folder_structure.srs_records_path)
        == "iterations/test_iteration/results/folio_srs_other_test_task.json"
    )

    assert str(folder_structure.statistical_codes_map_path) == "mapping_files/statcodes.tsv"

    assert str(folder_structure.temp_loan_type_map_path) == "mapping_files/temp_loan_types.tsv"

    assert (
        str(folder_structure.transformation_extra_data_path)
        == "iterations/test_iteration/results/extradata_test_task.extradata"
    )


def test_creates_subfolders(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    (base / ".gitignore").write_text("")

    folder_structure = FolderStructure(
        base, FOLIONamespaces.other, "test_task", "test_iteration", False
    )

    assert folder_structure.mapping_files_folder.is_dir()
    assert folder_structure.data_folder.is_dir()
    assert folder_structure.results_folder.is_dir()
    assert folder_structure.reports_folder.is_dir()
    assert folder_structure.raw_reports_folder.is_dir()


def test_base_folder_must_exist(tmp_path):
    missing_base = tmp_path / "missing"
    with pytest.raises(SystemExit):
        FolderStructure(
            missing_base, FOLIONamespaces.other, "test_task", "test_iteration", False
        )


@patch.object(FolderStructure, "verify_folder", fake_verify_folder)
def test_log_folder_structure(caplog):
    """Test that log_folder_structure logs the expected messages."""
    folder_structure = FolderStructure(
        "", FOLIONamespaces.other, "test_task", "test_iteration", False
    )
    folder_structure.setup_migration_file_structure()

    with caplog.at_level("INFO"):
        folder_structure.log_folder_structure()

    # Verify that the expected log messages were emitted
    assert "Mapping files folder is" in caplog.text
    assert "Base folder is" in caplog.text
    assert "Reports and logs folder is" in caplog.text
    assert "Results folder is" in caplog.text
    assert "Data folder is" in caplog.text
    assert "Source records files folder is" in caplog.text
    assert "Log file will be located at" in caplog.text
    assert "Data issue reports" in caplog.text


def test_verify_folder_path_is_file(tmp_path, caplog):
    """Test that verify_folder exits when path exists but is a file, not a directory."""
    # Create a file at the path where a directory should be
    file_as_folder = tmp_path / "should_be_dir"
    file_as_folder.touch()

    # Need a .gitignore file for FolderStructure to work
    (tmp_path / ".gitignore").write_text("")

    # Create a temporary FolderStructure with mocked verify for initial setup
    with patch.object(FolderStructure, "verify_folder", fake_verify_folder):
        folder_structure = FolderStructure(
            tmp_path, FOLIONamespaces.other, "test_task", "test_iteration", False
        )

    # Now call verify_folder directly with file path
    with caplog.at_level("CRITICAL"):
        with pytest.raises(SystemExit):
            folder_structure.verify_folder(file_as_folder)

    assert "Path exists but is not a directory" in caplog.text
