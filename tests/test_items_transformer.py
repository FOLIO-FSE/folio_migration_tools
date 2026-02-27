from unittest.mock import Mock
import uuid

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import IlsFlavour
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
    mock_config.boundwith_flavor = IlsFlavour.voyager
    items_transformer = Mock(spec=ItemsTransformer)
    items_transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    items_transformer.folder_structure.boundwith_relationships_map_path = "some_path"
    items_transformer.task_configuration = mock_config
    return items_transformer


def test_load_boundwith_relationships_happy_path(items_transformer):
    mock_data = '["key1", ["value1"]]\n["key2", ["value2"]]'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        ItemsTransformer._load_voyager_boundwith_relationships(items_transformer)
        assert len(items_transformer.boundwith_relationship_map) == 2
        assert items_transformer.boundwith_relationship_map["key1"] == ["value1"]


def test_load_boundwith_relationships_file_not_found(items_transformer):
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer._load_voyager_boundwith_relationships(items_transformer)
        assert (
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation not found."
               ) in str(exc_info.value)


def test_load_boundwith_relationships_invalid_json(items_transformer):
    mock_data = '["key1", ["value1"]\n["key2", ["value2"]]'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer._load_voyager_boundwith_relationships(items_transformer)
        assert (
               "Boundwith relationship file specified, but relationships file "
               "from holdings transformation is not a valid line JSON."
               ) in str(exc_info.value)


def test_load_boundwith_relationships_empty_file(items_transformer):
    mock_data = ''
    with patch("builtins.open", mock_open(read_data=mock_data)):
        ItemsTransformer._load_voyager_boundwith_relationships(items_transformer)
        assert len(items_transformer.boundwith_relationship_map) == 0


def test_load_boundwith_relationships_map_not_a_list(items_transformer):
    mock_data = '{"key1": ["value1"]}'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer._load_voyager_boundwith_relationships(items_transformer)
        assert (
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation is not a valid line JSON."
               ) in str(exc_info.value)


# --- Tests for load_boundwith_relationships dispatch ---


def test_load_boundwith_relationships_dispatches_to_voyager(items_transformer):
    ItemsTransformer.load_boundwith_relationships(items_transformer)
    items_transformer._load_voyager_boundwith_relationships.assert_called_once()


def test_load_boundwith_relationships_dispatches_to_aleph():
    mock_config = Mock(spec=ItemsTransformer.TaskConfiguration)
    mock_config.ecs_tenant_id = ""
    mock_config.name = "test"
    mock_config.boundwith_flavor = IlsFlavour.aleph
    transformer = Mock(spec=ItemsTransformer)
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.task_configuration = mock_config
    ItemsTransformer.load_boundwith_relationships(transformer)
    transformer._load_aleph_boundwith_relationships.assert_called_once()


def test_load_boundwith_relationships_unsupported_flavor():
    mock_config = Mock(spec=ItemsTransformer.TaskConfiguration)
    mock_config.ecs_tenant_id = ""
    mock_config.name = "test"
    mock_config.boundwith_flavor = IlsFlavour.sierra
    transformer = Mock(spec=ItemsTransformer)
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.task_configuration = mock_config
    with pytest.raises(TransformationProcessError) as exc_info:
        ItemsTransformer.load_boundwith_relationships(transformer)
    assert "Unsupported boundwith flavor:" in str(exc_info.value)
    assert "sierra" in str(exc_info.value)


# --- Tests for _load_aleph_boundwith_relationships ---


@pytest.fixture
def aleph_transformer():
    mock_config = Mock(spec=ItemsTransformer.TaskConfiguration)
    mock_config.ecs_tenant_id = ""
    mock_config.name = "test"
    mock_config.boundwith_flavor = IlsFlavour.aleph
    mock_config.boundwith_relationship_file_path = "boundwith.tsv"
    transformer = Mock(spec=ItemsTransformer)
    transformer.folder_structure = mocked_classes.get_mocked_folder_structure()
    transformer.task_configuration = mock_config
    transformer.task_config = mock_config
    transformer.boundwith_relationship_map = {}
    return transformer


def test_load_aleph_boundwith_relationships_happy_path(aleph_transformer):
    tsv_data = "LKR_HOL\tITEM_REC_KEY\nHOL001\tITEM001\n"
    with patch("builtins.open", mock_open(read_data=tsv_data)):
        ItemsTransformer._load_aleph_boundwith_relationships(aleph_transformer)
        assert "ITEM001" in aleph_transformer.boundwith_relationship_map
        assert "HOL001" in aleph_transformer.boundwith_relationship_map["ITEM001"]


def test_load_aleph_boundwith_relationships_file_not_found(aleph_transformer):
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(TransformationProcessError) as exc_info:
            ItemsTransformer._load_aleph_boundwith_relationships(aleph_transformer)
        assert "Boundwith relationship file specified, but file not found." in str(exc_info.value)


def test_load_aleph_boundwith_multiple_holdings_per_item(aleph_transformer):
    tsv_data = "LKR_HOL\tITEM_REC_KEY\nHOL001\tITEM001\nHOL002\tITEM001\n"
    with patch("builtins.open", mock_open(read_data=tsv_data)):
        ItemsTransformer._load_aleph_boundwith_relationships(aleph_transformer)
        assert "ITEM001" in aleph_transformer.boundwith_relationship_map
        assert aleph_transformer.boundwith_relationship_map["ITEM001"] == {"HOL001", "HOL002"}


def test_load_aleph_boundwith_empty_file(aleph_transformer):
    tsv_data = ""
    with patch("builtins.open", mock_open(read_data=tsv_data)):
        ItemsTransformer._load_aleph_boundwith_relationships(aleph_transformer)
        assert len(aleph_transformer.boundwith_relationship_map) == 0


# --- Tests for _validate_boundwith_relationships ---


def test_validate_boundwith_relationships_noop_for_voyager():
    mock_config = Mock(spec=ItemsTransformer.TaskConfiguration)
    mock_config.boundwith_flavor = IlsFlavour.voyager
    transformer = Mock(spec=ItemsTransformer)
    transformer.task_configuration = mock_config
    transformer.boundwith_relationship_map = {"ITEM001": {"HOL001"}}
    transformer.mapper = Mock()
    transformer.mapper.holdings_id_map = {}
    ItemsTransformer._validate_boundwith_relationships(transformer)
    # Map should be untouched since voyager skips validation
    assert transformer.boundwith_relationship_map == {"ITEM001": {"HOL001"}}


def test_validate_aleph_boundwith_all_valid(aleph_transformer):
    aleph_transformer.boundwith_relationship_map = {
        "ITEM001": {"HOL001", "HOL002"},
    }
    aleph_transformer.mapper = Mock()
    aleph_transformer.mapper.holdings_id_map = {"HOL001": "uuid-001", "HOL002": "uuid-002"}
    ItemsTransformer._validate_boundwith_relationships(aleph_transformer)
    assert aleph_transformer.boundwith_relationship_map["ITEM001"] == {"HOL001", "HOL002"}


def test_validate_aleph_boundwith_partial_valid(aleph_transformer):
    aleph_transformer.boundwith_relationship_map = {
        "ITEM001": {"HOL001", "HOL_MISSING"},
    }
    aleph_transformer.mapper = Mock()
    aleph_transformer.mapper.holdings_id_map = {"HOL001": "uuid-001"}
    with patch(
        "folio_migration_tools.migration_tasks.items_transformer.Helper.log_data_issue_failed"
    ) as mock_log:
        ItemsTransformer._validate_boundwith_relationships(aleph_transformer)
        mock_log.assert_called_once_with(
            "ITEM001",
            "Holdings for boundwith relationship not found in holdings id map",
            "HOL_MISSING",
        )
    assert aleph_transformer.boundwith_relationship_map["ITEM001"] == {"HOL001"}


def test_validate_aleph_boundwith_all_invalid_removes_entry(aleph_transformer):
    aleph_transformer.boundwith_relationship_map = {
        "ITEM001": {"HOL_MISSING"},
    }
    aleph_transformer.mapper = Mock()
    aleph_transformer.mapper.holdings_id_map = {}
    with patch(
        "folio_migration_tools.migration_tasks.items_transformer.Helper.log_data_issue_failed"
    ):
        ItemsTransformer._validate_boundwith_relationships(aleph_transformer)
    assert "ITEM001" not in aleph_transformer.boundwith_relationship_map


def test_validate_aleph_boundwith_empty_map(aleph_transformer):
    aleph_transformer.boundwith_relationship_map = {}
    aleph_transformer.mapper = Mock()
    aleph_transformer.mapper.holdings_id_map = {"HOL001": "uuid-001"}
    ItemsTransformer._validate_boundwith_relationships(aleph_transformer)
    assert len(aleph_transformer.boundwith_relationship_map) == 0
