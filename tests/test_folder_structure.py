from pathlib import Path
from unittest.mock import patch

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
        str(folder_structure.failed_bibs_file)
        == "iterations/test_iteration/results/failed_bib_records_test_task.mrc"
    )

    assert (
        str(folder_structure.failed_mfhds_file)
        == "iterations/test_iteration/results/failed_mfhd_records_test_task.mrc"
    )

    assert str(folder_structure.failed_recs_path).startswith(
        "iterations/test_iteration/results/failed_records_test_task_"
    )
    assert str(folder_structure.failed_recs_path).endswith(".txt")
    assert folder_structure.time_str in str(folder_structure.failed_recs_path)
    assert (
        str(folder_structure.holdings_id_map_path)
        == "iterations/test_iteration/results/holdings_id_map_test_iteration.json"
    )

    assert (
        str(folder_structure.instance_id_map_path)
        == "iterations/test_iteration/results/instance_id_map_test_iteration.json"
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
