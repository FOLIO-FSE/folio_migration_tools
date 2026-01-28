"""Tests for UserImporterTask adapter module."""

from types import MethodType
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
import json
import pytest

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.migration_tasks.user_importer import UserImportTask
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)


def test_get_object_type():
    """Test that get_object_type returns the expected namespace."""
    assert UserImportTask.get_object_type() == FOLIONamespaces.users


def test_task_configuration_defaults():
    """Test TaskConfiguration default values."""
    config = UserImportTask.TaskConfiguration(
        name="test_task",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="users.json")],
    )

    assert config.batch_size == 250
    assert config.user_match_key == "externalSystemId"
    assert config.only_update_present_fields is False
    assert config.default_preferred_contact_type == "002"
    assert config.fields_to_protect == []
    assert config.limit_simultaneous_requests == 10


def test_task_configuration_custom_values():
    """Test TaskConfiguration with custom values."""
    config = UserImportTask.TaskConfiguration(
        name="test_task",
        migration_task_type="UserImporterTask",
        files=[
            FileDefinition(file_name="users1.json"),
            FileDefinition(file_name="users2.json"),
        ],
        batch_size=100,
        user_match_key="username",
        only_update_present_fields=True,
        default_preferred_contact_type="email",
        fields_to_protect=["personal.email", "barcode", "patronGroup"],
        limit_simultaneous_requests=5,
    )

    assert len(config.files) == 2
    assert config.batch_size == 100
    assert config.user_match_key == "username"
    assert config.only_update_present_fields is True
    assert config.default_preferred_contact_type == "email"
    assert config.fields_to_protect == ["personal.email", "barcode", "patronGroup"]
    assert config.limit_simultaneous_requests == 5


def test_task_configuration_user_match_key_validation():
    """Test that user_match_key only accepts valid values."""
    valid_keys = ["externalSystemId", "username", "barcode"]

    for match_key in valid_keys:
        config = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="test.json")],
            user_match_key=match_key,
        )
        assert config.user_match_key == match_key


def test_task_configuration_preferred_contact_type_validation():
    """Test that default_preferred_contact_type accepts valid values."""
    # Test ID format
    valid_ids = ["001", "002", "003", "004", "005"]
    for contact_id in valid_ids:
        config = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="test.json")],
            default_preferred_contact_type=contact_id,
        )
        assert config.default_preferred_contact_type == contact_id

    # Test name format
    valid_names = ["mail", "email", "text", "phone", "mobile"]
    for name in valid_names:
        config = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="test.json")],
            default_preferred_contact_type=name,
        )
        assert config.default_preferred_contact_type == name


def test_task_configuration_batch_size_validation():
    """Test that batch_size is validated within range."""
    # Valid batch size
    config = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        batch_size=500,
    )
    assert config.batch_size == 500

    # Test boundary values
    config_min = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        batch_size=1,
    )
    assert config_min.batch_size == 1

    config_max = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        batch_size=1000,
    )
    assert config_max.batch_size == 1000


def test_task_configuration_simultaneous_requests_validation():
    """Test that limit_simultaneous_requests is validated within range."""
    config = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        limit_simultaneous_requests=50,
    )
    assert config.limit_simultaneous_requests == 50

    # Test boundary values
    config_min = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        limit_simultaneous_requests=1,
    )
    assert config_min.limit_simultaneous_requests == 1

    config_max = UserImportTask.TaskConfiguration(
        name="test",
        migration_task_type="UserImporterTask",
        files=[FileDefinition(file_name="test.json")],
        limit_simultaneous_requests=100,
    )
    assert config_max.limit_simultaneous_requests == 100


@pytest.fixture
def mock_folder_structure():
    """Create a mock folder structure."""
    folder_structure = Mock()
    folder_structure.results_folder = Path("/tmp/results")
    folder_structure.failed_recs_path = Path("/tmp/failed_records.json")
    folder_structure.migration_reports_file = Path("/tmp/report.md")
    folder_structure.migration_reports_raw_file = Path("/tmp/report.json")
    folder_structure.data_issue_file_path = Path("/tmp/data_issues.tsv")
    folder_structure.transformation_log_path = Path("/tmp/log.log")
    return folder_structure


@pytest.fixture
def mock_library_config():
    """Create a mock library configuration."""
    config = Mock(spec=LibraryConfiguration)
    config.library_name = "Test Library"
    config.base_folder = Path("/tmp")
    config.iteration_identifier = "test_iteration"
    config.add_time_stamp_to_file_names = False
    config.log_level_debug = False
    config.is_ecs = False
    config.ecs_tenant_id = None
    config.ecs_central_iteration_identifier = None
    return config


@pytest.fixture
def mock_folio_client():
    """Create a mock FOLIO client."""
    client = Mock(spec=FolioClient)
    client.okapi_url = "https://folio.example.com"
    client.tenant_id = "test_tenant"
    return client


class TestUserImporterTaskCreateFDIConfig:
    """Tests for the _create_fdi_config method."""

    def test_create_fdi_config_basic(self):
        """Test creating FDI config with basic settings."""
        task_config = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="test.json")],
        )

        # Create a minimal mock UserImporterTask to test the method
        importer = Mock(spec=UserImportTask)
        importer.task_configuration = task_config
        importer.library_configuration = Mock()
        importer.library_configuration.library_name = "Test Library"
        importer._create_fdi_config = MethodType(
            UserImportTask._create_fdi_config, importer
        )

        file_paths = [Path("/tmp/results/test.json")]
        fdi_config = importer._create_fdi_config(file_paths)

        assert fdi_config.library_name == "Test Library"
        assert fdi_config.batch_size == 250
        assert fdi_config.user_match_key == "externalSystemId"
        assert fdi_config.only_update_present_fields is False
        assert fdi_config.default_preferred_contact_type == "002"
        assert fdi_config.fields_to_protect == []
        assert fdi_config.limit_simultaneous_requests == 10
        assert fdi_config.user_file_paths == file_paths
        assert fdi_config.no_progress is False

    def test_create_fdi_config_with_protection(self):
        """Test creating FDI config with field protection."""
        task_config = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="test.json")],
            user_match_key="barcode",
            only_update_present_fields=True,
            fields_to_protect=["personal.email", "externalSystemId"],
            limit_simultaneous_requests=20,
        )

        importer = Mock(spec=UserImportTask)
        importer.task_configuration = task_config
        importer.library_configuration = Mock()
        importer.library_configuration.library_name = "My Library"
        importer._create_fdi_config = MethodType(
            UserImportTask._create_fdi_config, importer
        )

        file_paths = [Path("/tmp/users.json")]
        fdi_config = importer._create_fdi_config(file_paths)

        assert fdi_config.user_match_key == "barcode"
        assert fdi_config.only_update_present_fields is True
        assert fdi_config.fields_to_protect == ["personal.email", "externalSystemId"]
        assert fdi_config.limit_simultaneous_requests == 20


class TestUserImporterTaskMigrationReport:
    """Tests for migration report generation."""

    def test_translate_stats_to_migration_report(self):
        """Test that stats are properly translated to migration report."""
        from folio_data_import.UserImport import UserImporterStats

        # Create mock importer with stats
        importer = Mock(spec=UserImportTask)
        importer.task_configuration = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[
                FileDefinition(file_name="users1.json"),
                FileDefinition(file_name="users2.json"),
            ],
        )
        importer.stats = UserImporterStats(
            created=800,
            updated=150,
            failed=50,
        )
        importer.total_records = 1000
        importer.files_processed = ["users1.json", "users2.json"]
        importer.migration_report = Mock()
        importer._translate_stats_to_migration_report = MethodType(
            UserImportTask._translate_stats_to_migration_report, importer
        )

        importer._translate_stats_to_migration_report()

        # Verify set calls were made with correct values
        calls = importer.migration_report.set.call_args_list
        set_values = {(call[0][0], call[0][1]): call[0][2] for call in calls}

        assert set_values[("GeneralStatistics", "Total records in files")] == 1000
        assert set_values[("GeneralStatistics", "Records processed")] == 1000
        assert set_values[("GeneralStatistics", "Users created")] == 800
        assert set_values[("GeneralStatistics", "Users updated")] == 150
        assert set_values[("GeneralStatistics", "Users failed")] == 50

        # Verify files were added to report
        add_calls = importer.migration_report.add.call_args_list
        added_files = [call[0][1] for call in add_calls if call[0][0] == "FilesProcessed"]
        assert "users1.json" in added_files
        assert "users2.json" in added_files

    def test_translate_stats_all_successful(self):
        """Test stats translation when all users succeed."""
        from folio_data_import.UserImport import UserImporterStats

        importer = Mock(spec=UserImportTask)
        importer.task_configuration = UserImportTask.TaskConfiguration(
            name="test",
            migration_task_type="UserImporterTask",
            files=[FileDefinition(file_name="users.json")],
        )
        importer.stats = UserImporterStats(
            created=500,
            updated=500,
            failed=0,
        )
        importer.total_records = 1000
        importer.files_processed = ["users.json"]
        importer.migration_report = Mock()
        importer._translate_stats_to_migration_report = MethodType(
            UserImportTask._translate_stats_to_migration_report, importer
        )

        importer._translate_stats_to_migration_report()

        calls = importer.migration_report.set.call_args_list
        set_values = {(call[0][0], call[0][1]): call[0][2] for call in calls}

        assert set_values[("GeneralStatistics", "Users failed")] == 0
        assert set_values[("GeneralStatistics", "Users created")] == 500
        assert set_values[("GeneralStatistics", "Users updated")] == 500
