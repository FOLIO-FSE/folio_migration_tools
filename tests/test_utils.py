from pathlib import Path
from unittest.mock import MagicMock

from folio_migration_tools.library_configuration import (
    LibraryConfiguration,
    FolioRelease,
)


def get_mocked_library_config():
    return LibraryConfiguration(
        okapi_url="http://localhost:9130",
        tenant_id="test_tenant",
        okapi_username="test_user",
        okapi_password="test_password",
        base_folder=Path("."),
        library_name="Test Library",
        log_level_debug=False,
        folio_release=FolioRelease.sunflower,
        iteration_identifier="test_iteration"
    )


def get_mocked_folder_structure():
    mock_fs = MagicMock()
    mock_fs.mapping_files = Path("mapping_files")
    mock_fs.results_folder = Path("results")
    mock_fs.legacy_records_folder = Path("source_files")
    mock_fs.logs_folder = Path("logs")
    mock_fs.migration_reports_file = Path("/dev/null")
    mock_fs.transformation_extra_data_path = Path("transformation_extra_data")
    mock_fs.transformation_log_path = Path("/dev/null")
    mock_fs.data_issue_file_path = Path("/dev/null")
    mock_fs.failed_marc_recs_file = Path("failed_marc_recs.txt")
    return mock_fs
