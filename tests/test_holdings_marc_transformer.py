from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.migration_tasks.holdings_marc_transformer import HoldingsMarcTransformer
from folio_migration_tools.test_infrastructure.mocked_classes import (
    mocked_folio_client,
    get_mocked_library_config,
    get_mocked_folder_structure,
)


def test_get_object_type():
    assert HoldingsMarcTransformer.get_object_type() == FOLIONamespaces.holdings


def test_init_basic():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="test-uuid"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    with patch("builtins.open", mock_open(read_data="header1\theader2\nvalue1\tvalue2")):
        transformer = HoldingsMarcTransformer(
            task_config,
            mock_library_config,
            mock_folio,
            use_logging=False
        )
        assert transformer.task_config == task_config
        assert transformer.location_map is not None
        assert transformer.boundwith_relationship_map_rows == []


def test_init_with_invalid_holdings_type():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="non-existent-uuid"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    with pytest.raises(TransformationProcessError) as exc_info:
        with patch("builtins.open", mock_open(read_data="header1\theader2\nvalue1\tvalue2")):
            HoldingsMarcTransformer(
                task_config,
                mock_library_config,
                mock_folio,
                use_logging=False
            )
    assert "Holdings type with ID" in str(exc_info.value)


def test_init_with_boundwith_file():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="test-uuid",
        boundwith_relationship_file_path="boundwith.tsv"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    # Mock both file opens (location map and boundwith file)
    m = mock_open()
    m.side_effect = [
        mock_open(read_data="header1\theader2\nvalue1\tvalue2").return_value,
        mock_open(read_data="MFHD_ID\tBIB_ID\n1\t2").return_value
    ]

    with patch("builtins.open", m):
        transformer = HoldingsMarcTransformer(
            task_config,
            mock_library_config,
            mock_folio,
            use_logging=False
        )
        assert len(transformer.boundwith_relationship_map_rows) == 1


def test_init_with_missing_boundwith_file():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="test-uuid",
        boundwith_relationship_file_path="non_existent.tsv"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    with pytest.raises(TransformationProcessError) as exc_info:
        with patch("builtins.open") as mock_open_call:
            mock_open_call.side_effect = [
                mock_open(read_data="header1\theader2\nvalue1\tvalue2").return_value,
                FileNotFoundError()
            ]
            HoldingsMarcTransformer(
                task_config,
                mock_library_config,
                mock_folio,
                use_logging=False
            )
    assert "Provided boundwith relationship file not found" in str(exc_info.value)


def test_add_supplemental_mfhd_mappings():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="test-uuid",
        supplemental_mfhd_mapping_rules_file="rules.json"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    # Mock file operations for both location map and rules file
    m = mock_open()
    m.side_effect = [
        mock_open(read_data="header1\theader2\nvalue1\tvalue2").return_value,
        mock_open(read_data='{"test": "rule"}').return_value
    ]

    with patch("builtins.open", m):
        transformer = HoldingsMarcTransformer(
            task_config,
            mock_library_config,
            mock_folio,
            use_logging=False
        )
        transformer.mapper.integrate_supplemental_mfhd_mappings = Mock()
        transformer.add_supplemental_mfhd_mappings()
        transformer.mapper.integrate_supplemental_mfhd_mappings.assert_called_once_with({"test": "rule"})


def test_wrap_up():
    task_config = HoldingsMarcTransformer.TaskConfiguration(
        name="test_task",
        migration_task_type="holdings_marc",
        files=[FileDefinition(file_name="test.mrc")],
        legacy_id_marc_path="001",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="test-uuid"
    )

    mock_folio = mocked_folio_client()
    mock_library_config = get_mocked_library_config()

    with patch("builtins.open", mock_open(read_data="header1\theader2\nvalue1\tvalue2")):
        transformer = HoldingsMarcTransformer(
            task_config,
            mock_library_config,
            mock_folio,
            use_logging=False
        )
        
        # Mock necessary components
        transformer.extradata_writer = Mock()
        transformer.processor = Mock()
        transformer.mapper = Mock()
        transformer.mapper.boundwith_relationship_map = {"test": "relation"}
        
        # Test wrap_up
        with patch("builtins.open", mock_open()) as mock_file:
            transformer.wrap_up()
            
            # Verify method calls
            transformer.extradata_writer.flush.assert_called_once()
            transformer.processor.wrap_up.assert_called_once()
            transformer.mapper.migration_report.write_migration_report.assert_called_once()
