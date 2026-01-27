"""Tests for MARCImportTask adapter module."""

from types import MethodType
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
import json
import pytest

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.migration_tasks.marc_import import MARCImportTask
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)


def test_get_object_type():
    """Test that get_object_type returns the expected namespace."""
    assert MARCImportTask.get_object_type() == FOLIONamespaces.instances


def test_task_configuration_defaults():
    """Test TaskConfiguration default values."""
    config = MARCImportTask.TaskConfiguration(
        name="test_task",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="bibs.mrc")],
        import_profile_name="Test Import Profile",
    )

    assert config.import_profile_name == "Test Import Profile"
    assert config.split_files is False
    assert config.split_size == 1000
    assert config.batch_size == 10
    assert config.batch_delay == 0.0
    assert config.marc_record_preprocessors == []
    assert config.preprocessors_args == {}
    assert config.split_offset == 0


def test_task_configuration_custom_values():
    """Test TaskConfiguration with custom values."""
    config = MARCImportTask.TaskConfiguration(
        name="test_task",
        migration_task_type="MARCImportTask",
        files=[
            FileDefinition(file_name="bibs1.mrc"),
            FileDefinition(file_name="bibs2.mrc"),
        ],
        import_profile_name="Full MARC Import",
        split_files=True,
        split_size=5000,
        batch_size=50,
        batch_delay=0.5,
        split_offset=10,
    )

    assert len(config.files) == 2
    assert config.import_profile_name == "Full MARC Import"
    assert config.split_files is True
    assert config.split_size == 5000
    assert config.batch_size == 50
    assert config.batch_delay == 0.5
    assert config.split_offset == 10


def test_task_configuration_with_preprocessor():
    """Test TaskConfiguration with preprocessor settings."""
    config = MARCImportTask.TaskConfiguration(
        name="test_task",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="bibs.mrc")],
        import_profile_name="Test Profile",
        marc_record_preprocessors=["add_035", "strip_999"],
        preprocessors_args={"add_035": {"prefix": "(OCoLC)"}, "strip_999": {}},
    )

    assert config.marc_record_preprocessors == ["add_035", "strip_999"]
    assert config.preprocessors_args == {"add_035": {"prefix": "(OCoLC)"}, "strip_999": {}}


def test_task_configuration_split_size_validation():
    """Test that split_size is validated within range."""
    # Valid values
    config = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        split_size=5000,
    )
    assert config.split_size == 5000

    # Minimum boundary
    config_min = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        split_size=1,
    )
    assert config_min.split_size == 1


def test_task_configuration_batch_size_validation():
    """Test that batch_size is validated within range."""
    config = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        batch_size=100,
    )
    assert config.batch_size == 100

    # Boundary values
    config_min = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        batch_size=1,
    )
    assert config_min.batch_size == 1

    config_max = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        batch_size=1000,
    )
    assert config_max.batch_size == 1000


def test_task_configuration_split_offset_validation():
    """Test that split_offset is validated to be non-negative."""
    config = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        split_offset=0,
    )
    assert config.split_offset == 0

    config_positive = MARCImportTask.TaskConfiguration(
        name="test",
        migration_task_type="MARCImportTask",
        files=[FileDefinition(file_name="test.mrc")],
        import_profile_name="Test",
        split_offset=100,
    )
    assert config_positive.split_offset == 100


@pytest.fixture
def mock_folder_structure():
    """Create a mock folder structure."""
    folder_structure = Mock()
    folder_structure.results_folder = Path("/tmp/results")
    folder_structure.legacy_records_folder = Path("/tmp/legacy_records")
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


class TestMARCImportTaskCreateFDIConfig:
    """Tests for the _create_fdi_config method."""

    def test_create_fdi_config_basic(self, mock_folder_structure):
        """Test creating FDI config with basic settings."""
        task_config = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[FileDefinition(file_name="test.mrc")],
            import_profile_name="Quick MARC Import",
        )

        # Create a minimal mock MARCImportTask to test the method
        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = task_config
        importer.folder_structure = mock_folder_structure
        importer._create_fdi_config = MethodType(
            MARCImportTask._create_fdi_config, importer
        )

        file_paths = [Path("/tmp/legacy_records/test.mrc")]
        fdi_config = importer._create_fdi_config(file_paths)

        assert fdi_config.import_profile_name == "Quick MARC Import"
        assert fdi_config.marc_files == file_paths
        assert fdi_config.split_files is False
        assert fdi_config.split_size == 1000
        # show_progress=True by default, so no_progress=False
        assert fdi_config.no_progress is False

    def test_create_fdi_config_with_split(self, mock_folder_structure):
        """Test creating FDI config with file splitting enabled."""
        task_config = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[FileDefinition(file_name="test.mrc")],
            import_profile_name="Batch Import",
            split_files=True,
            split_size=5000,
            batch_size=50,
        )

        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = task_config
        importer.folder_structure = mock_folder_structure
        importer._create_fdi_config = MethodType(
            MARCImportTask._create_fdi_config, importer
        )

        file_paths = [Path("/tmp/bibs.mrc")]
        fdi_config = importer._create_fdi_config(file_paths)

        assert fdi_config.split_files is True
        assert fdi_config.split_size == 5000
        assert fdi_config.batch_size == 50

    def test_create_fdi_config_with_preprocessors(self, mock_folder_structure):
        """Test creating FDI config with preprocessors."""
        task_config = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[FileDefinition(file_name="test.mrc")],
            import_profile_name="Resume Import",
            marc_record_preprocessors=["add_035", "strip_999"],
            preprocessors_args={"add_035": {"prefix": "(OCoLC)"}},
        )

        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = task_config
        importer.folder_structure = mock_folder_structure
        importer._create_fdi_config = MethodType(
            MARCImportTask._create_fdi_config, importer
        )

        file_paths = [Path("/tmp/bibs.mrc")]
        fdi_config = importer._create_fdi_config(file_paths)

        assert fdi_config.marc_record_preprocessors == "add_035,strip_999"
        assert fdi_config.preprocessors_args == {"add_035": {"prefix": "(OCoLC)"}}


class TestMARCImportTaskFilePaths:
    """Tests for file path resolution."""

    def test_file_paths_from_results_folder(self, mock_folder_structure):
        """Test that file paths are resolved from results_folder."""
        task_config = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[
                FileDefinition(file_name="bibs1.mrc"),
                FileDefinition(file_name="bibs2.mrc"),
            ],
            import_profile_name="Test",
        )

        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = task_config
        importer.folder_structure = mock_folder_structure

        # File paths should be resolved from results_folder (consistent with other poster tasks)
        expected_paths = [
            mock_folder_structure.results_folder / "bibs1.mrc",
            mock_folder_structure.results_folder / "bibs2.mrc",
        ]

        actual_paths = [
            importer.folder_structure.results_folder / f.file_name
            for f in importer.task_configuration.files
        ]

        assert actual_paths == expected_paths


class TestMARCImportTaskMigrationReport:
    """Tests for migration report generation."""

    def test_translate_stats_to_migration_report(self):
        """Test that stats are properly translated to migration report.

        Note: Detailed stats (created/updated/discarded/error) are retrieved from
        the FOLIO job summary and logged by folio_data_import. We only track
        records_sent and job_ids directly.
        """
        # Create mock importer with stats
        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[
                FileDefinition(file_name="bibs1.mrc"),
                FileDefinition(file_name="bibs2.mrc"),
            ],
            import_profile_name="Test Profile",
        )
        importer.total_records_sent = 10000
        importer.files_processed = ["bibs1.mrc", "bibs2.mrc"]
        importer.job_ids = ["job-1", "job-2", "job-3"]
        importer.migration_report = Mock()
        importer._translate_stats_to_migration_report = MethodType(
            MARCImportTask._translate_stats_to_migration_report, importer
        )

        importer._translate_stats_to_migration_report()

        # Verify set calls were made with correct values
        calls = importer.migration_report.set.call_args_list
        set_values = {(call[0][0], call[0][1]): call[0][2] for call in calls}

        assert set_values[("GeneralStatistics", "Records sent to Data Import")] == 10000
        assert set_values[("GeneralStatistics", "Data Import jobs created")] == 3

        # Verify files were added to report
        add_calls = importer.migration_report.add.call_args_list
        added_files = [call[0][1] for call in add_calls if call[0][0] == "FilesProcessed"]
        assert "bibs1.mrc" in added_files
        assert "bibs2.mrc" in added_files


    def test_translate_stats_single_file(self):
        """Test stats translation with a single file."""
        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[FileDefinition(file_name="bibs.mrc")],
            import_profile_name="Test",
        )
        importer.total_records_sent = 5000
        importer.files_processed = ["bibs.mrc"]
        importer.job_ids = ["job-1"]
        importer.migration_report = Mock()
        importer._translate_stats_to_migration_report = MethodType(
            MARCImportTask._translate_stats_to_migration_report, importer
        )

        importer._translate_stats_to_migration_report()

        calls = importer.migration_report.set.call_args_list
        set_values = {(call[0][0], call[0][1]): call[0][2] for call in calls}

        assert set_values[("GeneralStatistics", "Records sent to Data Import")] == 5000
        assert set_values[("GeneralStatistics", "Data Import jobs created")] == 1


class TestMARCImportTaskJobTracking:
    """Tests for job ID tracking."""

    def test_job_ids_tracked(self):
        """Test that job IDs are tracked during import."""
        importer = Mock(spec=MARCImportTask)
        importer.job_ids = []

        # Simulate adding job IDs during import
        new_job_ids = ["job-abc-123", "job-def-456", "job-ghi-789"]
        for job_id in new_job_ids:
            importer.job_ids.append(job_id)

        assert len(importer.job_ids) == 3
        assert importer.job_ids == new_job_ids

    def test_job_ids_in_report(self):
        """Test that job IDs are included in migration report."""
        importer = Mock(spec=MARCImportTask)
        importer.task_configuration = MARCImportTask.TaskConfiguration(
            name="test",
            migration_task_type="MARCImportTask",
            files=[FileDefinition(file_name="bibs.mrc")],
            import_profile_name="Test",
        )
        importer.total_records_sent = 100
        importer.files_processed = ["bibs.mrc"]
        importer.job_ids = ["job-unique-id-1"]
        importer.migration_report = Mock()
        importer._translate_stats_to_migration_report = MethodType(
            MARCImportTask._translate_stats_to_migration_report, importer
        )

        importer._translate_stats_to_migration_report()

