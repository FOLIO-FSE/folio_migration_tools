from unittest.mock import Mock
import uuid

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from .test_infrastructure import mocked_classes
import json
from unittest.mock import mock_open, patch


def test_handle_circulation_notes_wrong_type():
    folio_rec = {
        "circulationNotes": [{"id": "someId", "noteType": "Check inn", "note": "some note"}]
    }
    with pytest.raises(TransformationProcessError):
        ItemsTransformer.handle_circulation_notes(folio_rec, str(uuid.uuid4()))


def test_handle_circulation_notes_no_note():
    folio_rec = {"circulationNotes": [{"id": "someId", "noteType": "Check in", "note": ""}]}
    ItemsTransformer.handle_circulation_notes(folio_rec, str(uuid.uuid4()))
    assert "circulationNotes" not in folio_rec


def test_handle_circulation_notes_happy_path():
    folio_rec = {
        "circulationNotes": [
            {"id": "someId", "noteType": "Check in", "note": "some_note"},
            {"id": "someId", "noteType": "Check in", "note": ""},
        ]
    }
    ItemsTransformer.handle_circulation_notes(folio_rec, str(uuid.uuid4()))
    assert folio_rec["circulationNotes"][0]["note"] == "some_note"
    assert len(folio_rec["circulationNotes"]) == 1


def test_get_object_type():
    assert ItemsTransformer.get_object_type() == FOLIONamespaces.items


@pytest.fixture
def items_transformer():
    mock_config = Mock(spec=ItemsTransformer.TaskConfiguration)
    mock_config.ecs_tenant_id = ""
    mock_config.name = "test"
    mock_config.boundwith_relationships_map_path = "some_path"
    items_transformer = Mock(spec=ItemsTransformer)
    items_transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    items_transformer.folder_structure.boundwith_relationships_map_path = "some_path"
    items_transformer.task_configuration = mock_config
    return items_transformer


def test_load_boundwith_relationships_happy_path(items_transformer):
    mock_data = '["key1", ["value1"]]\n["key2", ["value2"]]'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        ItemsTransformer.load_boundwith_relationships(items_transformer)
        assert len(items_transformer.boundwith_relationship_map) == 2
        assert items_transformer.boundwith_relationship_map["key1"] == ["value1"]


def test_load_boundwith_relationships_file_not_found(items_transformer):
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer.load_boundwith_relationships(items_transformer)
        assert (
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation not found."
               ) in str(exc_info.value)


def test_load_boundwith_relationships_invalid_json(items_transformer):
    mock_data = '["key1", ["value1"]\n["key2", ["value2"]]'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer.load_boundwith_relationships(items_transformer)
            assert (
                   "Boundwith relationship file specified, but relationships file "
                   "from holdings transformation is not a valid line JSON."
                   ) in str(exc_info.value)


def test_load_boundwith_relationships_empty_file(items_transformer):
    mock_data = ''
    with patch("builtins.open", mock_open(read_data=mock_data)):
        ItemsTransformer.load_boundwith_relationships(items_transformer)
        assert len(items_transformer.boundwith_relationship_map) == 0


def test_load_boundwith_relationships_map_not_a_list(items_transformer):
    mock_data = '{"key1": ["value1"]}'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer.load_boundwith_relationships(items_transformer)
        assert (
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation is not a valid line JSON."
               ) in str(exc_info.value)
