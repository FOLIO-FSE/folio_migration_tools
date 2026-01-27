"""Tests for BatchPosterV2 adapter module."""

from types import MethodType
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
import json
import pytest

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.migration_tasks.batch_poster_v2 import BatchPosterV2
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)


def test_get_object_type():
    """Test that get_object_type returns the expected namespace."""
    assert BatchPosterV2.get_object_type() == FOLIONamespaces.other


def test_task_configuration_defaults():
    """Test TaskConfiguration default values."""
    config = BatchPosterV2.TaskConfiguration(
        name="test_task",
        migration_task_type="BatchPosterV2",
        object_type="Instances",
        files=[FileDefinition(file_name="test.json")],
    )

    assert config.batch_size == 100
    assert config.upsert is False
    assert config.preserve_statistical_codes is False
    assert config.preserve_administrative_notes is False
    assert config.preserve_temporary_locations is False
    assert config.preserve_temporary_loan_types is False
    assert config.preserve_item_status is True
    assert config.patch_existing_records is False
    assert config.patch_paths == []
    assert config.rerun_failed_records is True


def test_task_configuration_custom_values():
    """Test TaskConfiguration with custom values."""
    config = BatchPosterV2.TaskConfiguration(
        name="test_task",
        migration_task_type="BatchPosterV2",
        object_type="Items",
        files=[
            FileDefinition(file_name="items1.json"),
            FileDefinition(file_name="items2.json"),
        ],
        batch_size=250,
        upsert=True,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=True,
        preserve_temporary_loan_types=True,
        preserve_item_status=False,
        patch_existing_records=True,
        patch_paths=["barcode", "status"],
        rerun_failed_records=False,
    )

    assert config.object_type == "Items"
    assert len(config.files) == 2
    assert config.batch_size == 250
    assert config.upsert is True
    assert config.preserve_statistical_codes is True
    assert config.preserve_administrative_notes is True
    assert config.preserve_temporary_locations is True
    assert config.preserve_temporary_loan_types is True
    assert config.preserve_item_status is False
    assert config.patch_existing_records is True
    assert config.patch_paths == ["barcode", "status"]
    assert config.rerun_failed_records is False


def test_task_configuration_object_type_validation():
    """Test that object_type only accepts valid values."""
    valid_types = ["Instances", "Holdings", "Items", "ShadowInstances"]

    for obj_type in valid_types:
        config = BatchPosterV2.TaskConfiguration(
            name="test",
            migration_task_type="BatchPosterV2",
            object_type=obj_type,
            files=[FileDefinition(file_name="test.json")],
        )
        assert config.object_type == obj_type


def test_task_configuration_batch_size_validation():
    """Test that batch_size is validated within range."""
    # Valid batch size
    config = BatchPosterV2.TaskConfiguration(
        name="test",
        migration_task_type="BatchPosterV2",
        object_type="Instances",
        files=[FileDefinition(file_name="test.json")],
        batch_size=500,
    )
    assert config.batch_size == 500

    # Test boundary values
    config_min = BatchPosterV2.TaskConfiguration(
        name="test",
        migration_task_type="BatchPosterV2",
        object_type="Instances",
        files=[FileDefinition(file_name="test.json")],
        batch_size=1,
    )
    assert config_min.batch_size == 1

    config_max = BatchPosterV2.TaskConfiguration(
        name="test",
        migration_task_type="BatchPosterV2",
        object_type="Instances",
        files=[FileDefinition(file_name="test.json")],
        batch_size=1000,
    )
    assert config_max.batch_size == 1000


@pytest.fixture
def mock_folder_structure():
    """Create a mock folder structure."""
    folder_structure = Mock()
    folder_structure.results_folder = Path("/tmp/results")
    folder_structure.failed_recs_path = Path("/tmp/failed_records.json")
    folder_structure.migration_reports_file = Path("/tmp/report.md")
    folder_structure.migration_reports_raw_file = Path("/tmp/report.json")
    folder_structure.data_issue_file_path = Path("/tmp/data_issues.tsv")
    folder_structure.failed_marc_recs_file = Path("/tmp/failed_marc.mrc")
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


class TestBatchPosterV2CreateFDIConfig:
    """Tests for the _create_fdi_config method."""

    def test_create_fdi_config_basic(self):
        """Test creating FDI config with basic settings."""
        task_config = BatchPosterV2.TaskConfiguration(
            name="test",
            migration_task_type="BatchPosterV2",
            object_type="Instances",
            files=[FileDefinition(file_name="test.json")],
            batch_size=100,
        )

        # Create a minimal mock BatchPosterV2 to test the method
        poster = Mock(spec=BatchPosterV2)
        poster.task_configuration = task_config
        poster._create_fdi_config = MethodType(BatchPosterV2._create_fdi_config, poster)

        fdi_config = poster._create_fdi_config()

        assert fdi_config.object_type == "Instances"
        assert fdi_config.batch_size == 100
        assert fdi_config.upsert is False
        assert fdi_config.no_progress is False

    def test_create_fdi_config_upsert_options(self):
        """Test creating FDI config with upsert options."""
        task_config = BatchPosterV2.TaskConfiguration(
            name="test",
            migration_task_type="BatchPosterV2",
            object_type="Items",
            files=[FileDefinition(file_name="test.json")],
            upsert=True,
            preserve_statistical_codes=True,
            preserve_administrative_notes=True,
            preserve_temporary_locations=True,
            preserve_temporary_loan_types=True,
            preserve_item_status=False,
            patch_existing_records=True,
            patch_paths=["barcode", "status"],
        )

        poster = Mock(spec=BatchPosterV2)
        poster.task_configuration = task_config
        poster._create_fdi_config = MethodType(BatchPosterV2._create_fdi_config, poster)

        fdi_config = poster._create_fdi_config()

        assert fdi_config.object_type == "Items"
        assert fdi_config.upsert is True
        assert fdi_config.preserve_statistical_codes is True
        assert fdi_config.preserve_administrative_notes is True
        assert fdi_config.preserve_temporary_locations is True
        assert fdi_config.preserve_temporary_loan_types is True
        assert fdi_config.preserve_item_status is False
        assert fdi_config.patch_existing_records is True
        assert fdi_config.patch_paths == ["barcode", "status"]


class TestBatchPosterV2MigrationReport:
    """Tests for migration report generation."""

    def test_translate_stats_to_migration_report(self):
        """Test that stats are properly translated to migration report."""
        from folio_data_import.BatchPoster import BatchPosterStats

        # Create mock poster with stats
        poster = Mock(spec=BatchPosterV2)
        poster.task_configuration = BatchPosterV2.TaskConfiguration(
            name="test",
            migration_task_type="BatchPosterV2",
            object_type="Instances",
            files=[FileDefinition(file_name="test.json")],
            rerun_failed_records=True,
        )
        poster.stats = BatchPosterStats(
            records_processed=1000,
            records_posted=950,
            records_created=800,
            records_updated=150,
            records_failed=50,
            batches_posted=10,
            batches_failed=1,
            rerun_succeeded=30,
            rerun_still_failed=20,
        )
        poster.migration_report = Mock()
        poster._translate_stats_to_migration_report = MethodType(
            BatchPosterV2._translate_stats_to_migration_report, poster
        )

        poster._translate_stats_to_migration_report()

        # Verify set calls were made with correct values
        calls = poster.migration_report.set.call_args_list
        set_values = {
            (call[0][0], call[0][1]): call[0][2] for call in calls
        }

        assert set_values[("GeneralStatistics", "Records processed")] == 1000
        assert set_values[("GeneralStatistics", "Records posted successfully")] == 950
        assert set_values[("GeneralStatistics", "Records created")] == 800
        assert set_values[("GeneralStatistics", "Records updated")] == 150
        assert set_values[("GeneralStatistics", "Records failed")] == 50
        assert set_values[("GeneralStatistics", "Batches posted")] == 10
        assert set_values[("GeneralStatistics", "Batches failed")] == 1
        assert set_values[("GeneralStatistics", "Rerun succeeded")] == 30
        assert set_values[("GeneralStatistics", "Rerun still failed")] == 20

    def test_translate_stats_without_rerun(self):
        """Test stats translation when rerun is disabled."""
        from folio_data_import.BatchPoster import BatchPosterStats

        poster = Mock(spec=BatchPosterV2)
        poster.task_configuration = BatchPosterV2.TaskConfiguration(
            name="test",
            migration_task_type="BatchPosterV2",
            object_type="Holdings",
            files=[FileDefinition(file_name="test.json")],
            rerun_failed_records=False,
        )
        poster.stats = BatchPosterStats(
            records_processed=500,
            records_posted=500,
            records_created=500,
            records_updated=0,
            records_failed=0,
            batches_posted=5,
            batches_failed=0,
        )
        poster.migration_report = Mock()
        poster._translate_stats_to_migration_report = MethodType(
            BatchPosterV2._translate_stats_to_migration_report, poster
        )

        poster._translate_stats_to_migration_report()

        # Verify rerun stats are not included
        calls = poster.migration_report.set.call_args_list
        call_keys = [(call[0][0], call[0][1]) for call in calls]

        assert ("GeneralStatistics", "Rerun succeeded") not in call_keys
        assert ("GeneralStatistics", "Rerun still failed") not in call_keys
