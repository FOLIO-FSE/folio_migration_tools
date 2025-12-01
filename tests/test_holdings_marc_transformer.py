from unittest.mock import Mock, mock_open, patch
import pytest
from pathlib import Path
from .test_infrastructure import mocked_classes

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)
from folio_migration_tools.custom_exceptions import TransformationProcessError


def test_get_object_type():
    assert HoldingsMarcTransformer.get_object_type() == FOLIONamespaces.holdings


@pytest.fixture
def holdings_marc_transformer():
    mock_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mock_config.ecs_tenant_id = ""
    mock_config.name = "test"
    mock_config.boundwith_relationships_map_path = "some_path"
    marc_transformer = Mock(spec=HoldingsMarcTransformer)
    marc_transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    marc_transformer.folder_structure.boundwith_relationships_map_path = "some_path"
    marc_transformer.task_configuration = mock_config
    return marc_transformer


def test_add_supplemental_mfhd_mappings_happy_path():
    mock_data = '{"001": "value1", "002": "value2"}'
    transformer = Mock(spec=HoldingsMarcTransformer)
    transformer.task_configuration = Mock()
    transformer.task_configuration.supplemental_mfhd_mapping_rules_file = "dummy.json"
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.folder_structure.mapping_files_folder = Path(".")
    transformer.mapper = Mock()
    with patch("builtins.open", mock_open(read_data=mock_data)):
        HoldingsMarcTransformer.add_supplemental_mfhd_mappings(transformer)
        transformer.mapper.integrate_supplemental_mfhd_mappings.assert_called_once_with(
            {"001": "value1", "002": "value2"}
        )


def test_add_supplemental_mfhd_mappings_empty_file():
    transformer = Mock(spec=HoldingsMarcTransformer)
    transformer.task_configuration = Mock()
    transformer.task_configuration.supplemental_mfhd_mapping_rules_file = ""
    transformer.mapper = Mock()
    HoldingsMarcTransformer.add_supplemental_mfhd_mappings(transformer)
    transformer.mapper.integrate_supplemental_mfhd_mappings.assert_called_once_with({})


def test_add_supplemental_mfhd_mappings_invalid_json():
    mock_data = '{"001": "value1", "002": "value2"'
    transformer = Mock(spec=HoldingsMarcTransformer)
    transformer.task_configuration = Mock()
    transformer.task_configuration.supplemental_mfhd_mapping_rules_file = "dummy.json"
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.folder_structure.mapping_files_folder = Path(".")
    transformer.mapper = Mock()
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with pytest.raises(Exception):
            HoldingsMarcTransformer.add_supplemental_mfhd_mappings(transformer)


def test_add_supplemental_mfhd_mappings_file_not_found():
    transformer = Mock(spec=HoldingsMarcTransformer)
    transformer.task_configuration = Mock()
    transformer.task_configuration.supplemental_mfhd_mapping_rules_file = "dummy.json"
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.folder_structure.mapping_files_folder = Path(".")
    transformer.mapper = Mock()
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(TransformationProcessError):
            HoldingsMarcTransformer.add_supplemental_mfhd_mappings(transformer)
