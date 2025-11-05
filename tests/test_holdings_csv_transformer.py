import logging
from unittest.mock import Mock

from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
    explode_former_ids
)
from folio_migration_tools.library_configuration import (
    FileDefinition
)
import csv
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.test_infrastructure import mocked_classes
from folio_uuid.folio_namespaces import FOLIONamespaces
from pathlib import Path
import pytest
import json

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True

# HoldingsCsvTransformer.load_mapped_fields(mock_transformer)



def test_load_ref_data_mapping_file_valid_file():
    csv.register_dialect("tsv", delimiter="\t")
    valid_file = Path("tests/test_data/ref_data_maps/locations_map_valid.tsv")
    ref_data_map = HoldingsCsvTransformer.load_ref_data_mapping_file("permanentLocationId", valid_file, ["permanentLocationId"])
    assert ref_data_map == [{'folio_code': 'STACKS', 'legacy_code': 'stacks'}, {'folio_code': 'VAULT', 'legacy_code': 'vault'}, {'folio_code': 'REF', 'legacy_code': 'ref'}]


def test_load_ref_data_mapping_file_file_not_found():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("non_existent_folder")

    with pytest.raises(FileNotFoundError):
        HoldingsCsvTransformer.load_ref_data_mapping_file(
            "field", "non_existent_file.csv", ["field"], False
        )


def test_load_ref_data_mapping_file_invalid_json():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("")

    # Mock the open function to return invalid JSON content
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("builtins.open", lambda *args, **kwargs: Mock(read=lambda: "invalid_json"))
        with pytest.raises(Exception):
            HoldingsCsvTransformer.load_ref_data_mapping_file(
                "field", "invalid_file.csv", ["field"], False
            )


def test_load_ref_data_mapping_file_missing_required_field():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("")

    # Mock the file content with missing required fields
    invalid_file_content = [
        {"folio_field": "folio_value_1"},  # Missing "legacy_field"
    ]

    # Mock the open function to return the invalid file content
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "builtins.open",
            lambda *args, **kwargs: Mock(read=lambda: json.dumps(invalid_file_content)),
        )
        with pytest.raises(Exception):
            HoldingsCsvTransformer.load_ref_data_mapping_file(
                "field", "invalid_file.csv", ["field"], False
            )

def test_get_object_type():
    assert HoldingsCsvTransformer.get_object_type() == FOLIONamespaces.holdings


def test_generate_boundwith_part(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper

    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_mapper.base_string_for_folio_uuid = mocked_classes.mocked_folio_client().gateway_url
    HoldingsMapper.create_and_write_boundwith_part(mock_mapper, "legacy_id", "holding_uuid")

    assert any("boundwithPart\t" in ed for ed in mock_mapper.extradata_writer.cache)
    assert any(
        '"itemId": "d5cba195-882f-5d70-9195-8d22ad2920dd"}\n' in ed
        for ed in mock_mapper.extradata_writer.cache
    )
    assert any(
        '"holdingsRecordId": "holding_uuid"' in ed for ed in mock_mapper.extradata_writer.cache
    )


def test_merge_holding_in_first_boundwith(caplog):
    mock_folio = mocked_classes.mocked_folio_client()

    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.folio_client = mock_folio
    mock_transformer.mapper = mock_mapper

    new_holding = {"id": "holdings_id", "instanceId": "Instance_1", "permanentLocationId": "loc_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)
    assert len(mock_transformer.holdings) == 1
    assert "bw_Instance_1_loc_1__Instance_1_Instance_2" in mock_transformer.bound_with_keys


def test_merge_holding_in_second_boundwith_to_merge(caplog):
    mock_folio = mocked_classes.mocked_folio_client()

    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.folio_client = mock_folio
    mock_transformer.mapper = mock_mapper
    mock_transformer.object_type = FOLIONamespaces.holdings
    new_holding = {"id": "holdings_id", "instanceId": "Instance_1", "permanentLocationId": "loc_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)

    new_holding_2 = {"instanceId": "Instance_1", "permanentLocationId": "loc_1"}
    instance_ids_2: list[str] = ["Instance_1", "Instance_2"]
    item_id_2: str = "item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, new_holding_2, instance_ids_2, item_id_2
    )
    assert len(mock_transformer.holdings) == 1
    assert len(mock_transformer.bound_with_keys) == 1
    assert "bw_Instance_1_loc_1__Instance_1_Instance_2" in mock_transformer.bound_with_keys


def test_merge_holding_in_second_boundwith_to_not_merge(caplog):
    mock_folio = mocked_classes.mocked_folio_client()

    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.folio_client = mock_folio
    mock_transformer.mapper = mock_mapper

    new_holding = {"id": "holdings_id", "instanceId": "Instance_1", "permanentLocationId": "loc_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)

    new_holding_2 = {
        "id": "holdings_id2",
        "instanceId": "Instance_2",
        "permanentLocationId": "loc_1",
    }
    instance_ids_2: list[str] = ["Instance_3", "Instance_2"]
    item_id_2: str = "item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, new_holding_2, instance_ids_2, item_id_2
    )
    assert len(mock_transformer.holdings) == 2
    assert len(mock_transformer.bound_with_keys) == 2
    assert "bw_Instance_2_loc_1__Instance_2_Instance_3" in mock_transformer.bound_with_keys


def test_merge_holding_in_second_boundwith_different_locs_no_merge(caplog):
    mock_folio = mocked_classes.mocked_folio_client()

    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.folio_client = mock_folio
    mock_transformer.mapper = mock_mapper

    new_holding = {"id": "holdings_id", "instanceId": "Instance_1", "permanentLocationId": "loc_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)

    new_holding_2 = {
        "id": "holdings_id2",
        "instanceId": "Instance_2",
        "permanentLocationId": "loc_2",
        "callNumber": "call_number",
    }
    instance_ids_2: list[str] = ["Instance_1", "Instance_2"]
    item_id_2: str = "item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, new_holding_2, instance_ids_2, item_id_2
    )
    assert len(mock_transformer.holdings) == 2
    assert len(mock_transformer.bound_with_keys) == 2
    assert "bw_Instance_1_loc_1__Instance_1_Instance_2" in mock_transformer.bound_with_keys
    assert (
        "bw_Instance_2_loc_2_call_number_Instance_1_Instance_2" in mock_transformer.bound_with_keys
    )


def test_load_call_number_type_map_file_not_found():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.call_number_type_map_file_name = "non_existent_file.csv"
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("")
    with pytest.raises(FileNotFoundError):
        HoldingsCsvTransformer.load_call_number_type_map = Mock(
            side_effect=FileNotFoundError("File not found")
        )
        HoldingsCsvTransformer.load_call_number_type_map(mock_transformer)


def test_load_location_map_file_not_found():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.location_map_file_name = "non_existent_file.csv"
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("")
    with pytest.raises(FileNotFoundError):
        HoldingsCsvTransformer.load_location_map = Mock(
            side_effect=FileNotFoundError("File not found")
        )
        HoldingsCsvTransformer.load_location_map(mock_transformer)


def test_load_mapped_fields_file_not_found():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.holdings_map_file_name = "non_existent_file.csv"
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = Path("")


def test_merge_holding_in_boundwith_new_holding():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.object_type = FOLIONamespaces.holdings

    incoming_holding = {
        "id": "holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
        "callNumber": "call_1",
    }
    instance_ids = ["Instance_1", "Instance_2"]
    legacy_item_id = "legacy_item_1"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.bound_with_keys) == 1
    assert len(mock_transformer.holdings) == 1
    assert "bw_Instance_1_loc_1_call_1_Instance_1_Instance_2" in mock_transformer.bound_with_keys
    mock_transformer.mapper.create_and_write_boundwith_part.assert_called_once_with(
        legacy_item_id, incoming_holding["id"]
    )


def test_merge_holding_in_boundwith_existing_holding():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = {"bw_Instance_1_loc_1_call_1_Instance_1_Instance_2"}
    mock_transformer.holdings = {
        "bw_Instance_1_loc_1_call_1_Instance_1_Instance_2": {
            "id": "existing_holding_id",
            "instanceId": "Instance_1",
            "permanentLocationId": "loc_1",
            "callNumber": "call_1",
        }
    }
    mock_transformer.holdings_id_map = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.object_type = FOLIONamespaces.holdings

    incoming_holding = {
        "id": "new_holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
        "callNumber": "call_1",
    }
    instance_ids = ["Instance_1", "Instance_2"]
    legacy_item_id = "legacy_item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.bound_with_keys) == 1
    assert len(mock_transformer.holdings) == 1
    mock_transformer.mapper.create_and_write_boundwith_part.assert_called_once_with(
        legacy_item_id, "existing_holding_id"
    )


def test_merge_holding_in_regular_holding_new():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.holdings = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.task_configuration = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.task_configuration.holdings_merge_criteria = [
        "instanceId",
        "permanentLocationId",
    ]
    mock_transformer.task_configuration.holdings_type_uuid_for_boundwiths = ""

    incoming_holding = {
        "id": "holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
    }
    instance_ids = ["Instance_1"]
    legacy_item_id = "legacy_item_1"

    merge_holding = HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )
    print("merge_holding", merge_holding)

    assert len(mock_transformer.holdings) == 1
    assert "Instance_1-loc_1" in mock_transformer.holdings
    mock_transformer.mapper.migration_report.add_general_statistics.assert_called_with(
        "Unique Holdings created from Items"
    )


def test_merge_holding_in_regular_holding_existing():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.holdings = {
        "Instance_1_loc_1": {
            "id": "existing_holding_id",
            "instanceId": "Instance_1",
            "permanentLocationId": "loc_1",
        }
    }
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.task_configuration = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mock_transformer.task_configuration.holdings_merge_criteria = [
        "instanceId",
        "permanentLocationId",
    ]
    mock_transformer.task_configuration.holdings_type_uuid_for_boundwiths = ""

    incoming_holding = {
        "id": "new_holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
    }
    instance_ids = ["Instance_1"]
    legacy_item_id = "legacy_item_1"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.holdings) == 1
    mock_transformer.mapper.migration_report.add_general_statistics.assert_called_with(
        "Holdings already created from Item"
    )
    mock_transformer.load_mapped_fields = Mock(side_effect=FileNotFoundError("File not found"))
    with pytest.raises(FileNotFoundError):
        HoldingsCsvTransformer.load_mapped_fields(mock_transformer)


def test_merge_holding_in_boundwith_new_holding():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.object_type = FOLIONamespaces.holdings

    incoming_holding = {
        "id": "holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
        "callNumber": "call_1",
    }
    instance_ids = ["Instance_1", "Instance_2"]
    legacy_item_id = "legacy_item_1"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.bound_with_keys) == 1
    assert len(mock_transformer.holdings) == 1
    assert "bw_Instance_1_loc_1_call_1_Instance_1_Instance_2" in mock_transformer.bound_with_keys
    mock_transformer.mapper.create_and_write_boundwith_part.assert_called_once_with(
        legacy_item_id, incoming_holding["id"]
    )


def test_merge_holding_in_boundwith_existing_holding():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = {"bw_Instance_1_loc_1_call_1_Instance_1_Instance_2"}
    mock_transformer.holdings = {
        "bw_Instance_1_loc_1_call_1_Instance_1_Instance_2": {
            "id": "existing_holding_id",
            "instanceId": "Instance_1",
            "permanentLocationId": "loc_1",
            "callNumber": "call_1",
        }
    }
    mock_transformer.holdings_id_map = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.object_type = FOLIONamespaces.holdings

    incoming_holding = {
        "id": "new_holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
        "callNumber": "call_1",
    }
    instance_ids = ["Instance_1", "Instance_2"]
    legacy_item_id = "legacy_item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.bound_with_keys) == 1
    assert len(mock_transformer.holdings) == 1
    mock_transformer.mapper.create_and_write_boundwith_part.assert_called_once_with(
        legacy_item_id, "existing_holding_id"
    )


def test_merge_holding_in_regular_holding_new():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.holdings = {}
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.task_configuration = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.task_configuration.holdings_merge_criteria = [
        "instanceId",
        "permanentLocationId",
    ]
    mock_transformer.task_configuration.holdings_type_uuid_for_boundwiths = ""

    incoming_holding = {
        "id": "holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
    }
    instance_ids = ["Instance_1"]
    legacy_item_id = "legacy_item_1"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.holdings) == 1
    assert "Instance_1-loc_1" in mock_transformer.holdings
    mock_transformer.mapper.migration_report.add_general_statistics.assert_called_with(
        "Unique Holdings created from Items"
    )


def test_merge_holding_in_regular_holding_existing():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.holdings = {
        "Instance_1-loc_1": {
            "id": "existing_holding_id",
            "instanceId": "Instance_1",
            "permanentLocationId": "loc_1",
        }
    }
    mock_transformer.mapper = Mock(spec=HoldingsMapper)
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.task_configuration = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mock_transformer.task_configuration.holdings_merge_criteria = [
        "instanceId",
        "permanentLocationId",
    ]
    mock_transformer.task_configuration.holdings_type_uuid_for_boundwiths = ""

    incoming_holding = {
        "id": "new_holding_id",
        "instanceId": "Instance_1",
        "permanentLocationId": "loc_1",
    }
    instance_ids = ["Instance_1"]
    legacy_item_id = "legacy_item_1"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, incoming_holding, instance_ids, legacy_item_id
    )

    assert len(mock_transformer.holdings) == 1
    mock_transformer.mapper.migration_report.add_general_statistics.assert_called_with(
        "Holdings already created from Item"
    )

def test_post_process_holding_default_call_number_type():
    # Setup mock transformer
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.default_call_number_type = {"id": "default-call-number-type-id"}
    mock_transformer.fallback_holdings_type = {"id": "fallback-holdings-type-id"}
    mock_transformer.mapper = Mock()
    
    # Create test folio record without callNumberTypeId
    folio_rec = {
        "instanceId": ["test-instance-id"],
        "permanentLocationId": "test-location"
    }
    
    # Call post_process_holding
    HoldingsCsvTransformer.post_process_holding(
        mock_transformer,
        folio_rec,
        "test-legacy-id",
        Mock()
    )
    
    # Assert default call number type was set
    assert folio_rec["callNumberTypeId"] == "default-call-number-type-id"

def test_post_process_holding_existing_call_number_type():
    # Setup mock transformer
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.default_call_number_type = {"id": "default-call-number-type-id"}
    mock_transformer.fallback_holdings_type = {"id": "fallback-holdings-type-id"}
    mock_transformer.mapper = Mock()
    
    # Create test folio record with existing callNumberTypeId
    folio_rec = {
        "instanceId": ["test-instance-id"],
        "permanentLocationId": "test-location",
        "callNumberTypeId": "existing-call-number-type-id"
    }
    
    # Call post_process_holding
    HoldingsCsvTransformer.post_process_holding(
        mock_transformer,
        folio_rec,
        "test-legacy-id",
        Mock()
    )
    
    # Assert existing call number type was preserved
    assert folio_rec["callNumberTypeId"] == "existing-call-number-type-id"

def test_post_process_holding_empty_call_number_type():
    # Setup mock transformer
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.default_call_number_type = {"id": "default-call-number-type-id"}
    mock_transformer.fallback_holdings_type = {"id": "fallback-holdings-type-id"}
    mock_transformer.mapper = Mock()
    
    # Create test folio record with empty callNumberTypeId
    folio_rec = {
        "instanceId": ["test-instance-id"],
        "permanentLocationId": "test-location",
        "callNumberTypeId": ""
    }
    
    # Call post_process_holding
    HoldingsCsvTransformer.post_process_holding(
        mock_transformer,
        folio_rec,
        "test-legacy-id",
        Mock()
    )
    
    # Assert default call number type was set
    assert folio_rec["callNumberTypeId"] == "default-call-number-type-id"

def test_task_configuration_validation():
    # Test required fields
    config_data = {
        "name": "test_task",
        "migration_task_type": "holdings",
        "hrid_handling": "default",
        "files": [],
        "holdings_map_file_name": "holdings_map.json",
        "location_map_file_name": "locations.tsv",
        "default_call_number_type_name": "LC",
        "fallback_holdings_type_id": "12345"
    }
    config = HoldingsCsvTransformer.TaskConfiguration(**config_data)
    assert config.name == "test_task"
    assert config.migration_task_type == "holdings"
    assert config.holdings_map_file_name == "holdings_map.json"

def test_validate_merge_criteria_valid():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.holdings_merge_criteria = ["instanceId", "permanentLocationId"]
    mock_transformer.folio_client = Mock()
    
    # Mock the holdings schema
    mock_transformer.folio_client.get_holdings_schema.return_value = {
        "properties": {
            "instanceId": {},
            "permanentLocationId": {},
            "callNumber": {}
        }
    }
    
    # Should not raise any exception
    HoldingsCsvTransformer.validate_merge_criteria(mock_transformer)

def test_validate_merge_criteria_invalid():
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.holdings_merge_criteria = ["invalidField"]
    mock_transformer.folio_client = Mock()
    
    # Mock the holdings schema
    mock_transformer.folio_client.get_holdings_schema.return_value = {
        "properties": {
            "instanceId": {},
            "permanentLocationId": {},
            "callNumber": {}
        }
    }
    
    with pytest.raises(SystemExit):
        HoldingsCsvTransformer.validate_merge_criteria(mock_transformer)

def test_process_single_file_success(tmp_path):
    # Create a mock file
    items_dir = tmp_path / "items"
    items_dir.mkdir(exist_ok=True)
    test_file = items_dir / "test.csv"
    test_file.write_text("header1,header2\nvalue1,value2")
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.data_folder = tmp_path
    mock_transformer.mapper = Mock()
    mock_transformer.mapper.migration_report = Mock()
    mock_transformer.mapper.get_objects.return_value = [{"field": "value"}]
    
    file_def = Mock(spec=FileDefinition)
    file_def.file_name = "test.csv"
    
    HoldingsCsvTransformer.process_single_file(mock_transformer, file_def)
    
    mock_transformer.mapper.migration_report.add_general_statistics.assert_called()
    assert mock_transformer.total_records == 1

def test_explode_former_ids():
    # Test with array-like string
    holding_with_array = {"formerIds": ["[id1,id2,id3]"]}
    result = explode_former_ids(holding_with_array)
    assert result == ["id1", "id2", "id3"]
    
    # Test with regular string
    holding_with_string = {"formerIds": ["regular_id"]}
    result = explode_former_ids(holding_with_string)
    assert result == ["regular_id"]
    
    # Test with mixed content
    holding_mixed = {"formerIds": ["[id1,id2]", "regular_id", "[id3,id4]"]}
    result = explode_former_ids(holding_mixed)
    assert result == ["id1", "id2", "regular_id", "id3", "id4"]


def test_load_call_number_type_map_default(tmp_path, monkeypatch):
    # When the call number type map file does not exist, default mapping is returned
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = tmp_path

    mock_task_conf = Mock()
    mock_task_conf.call_number_type_map_file_name = "nonexistent.tsv"
    mock_task_conf.default_call_number_type_name = "DefaultCN"
    mock_transformer.task_configuration = mock_task_conf

    # holdings_field_map must have at least one mapped legacy field not equal to 'not mapped'
    mock_transformer.holdings_field_map = {"data": [{"legacy_field": "legacy1"}, {"legacy_field": "not mapped"}]}

    # ensure os.path.isfile behaves consistently across environments
    monkeypatch.setattr("os.path.isfile", lambda _: False)
    result = HoldingsCsvTransformer.load_call_number_type_map(mock_transformer)
    # Expect the returned mapping to contain the legacy key and default folio name
    assert isinstance(result, list)
    assert result[0]["legacy1"] == "*"
    assert result[0]["folio_name"] == "DefaultCN"


def test_load_call_number_type_map_file_exists(tmp_path, monkeypatch):
    # When a mapping file exists, the instance's load_ref_data_mapping_file should be used
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = tmp_path

    # create an empty file to satisfy isfile check
    fname = "cn_map.tsv"
    (tmp_path / fname).write_text("dummy")

    mock_task_conf = Mock()
    mock_task_conf.call_number_type_map_file_name = fname
    mock_transformer.task_configuration = mock_task_conf

    mock_transformer.folio_keys = ["a"]
    expected = [{"folio_code": "FC", "legacy_code": "LC"}]
    mock_transformer.load_ref_data_mapping_file = Mock(return_value=expected)

    monkeypatch.setattr("os.path.isfile", lambda _: True)
    result = HoldingsCsvTransformer.load_call_number_type_map(mock_transformer)
    mock_transformer.load_ref_data_mapping_file.assert_called()
    assert result == expected


def test_load_location_map_file_exists(tmp_path, monkeypatch):
    # When a location map file exists, load_ref_data_mapping_file is called
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = tmp_path

    fname = "locations.tsv"
    (tmp_path / fname).write_text("dummy")

    mock_task_conf = Mock()
    mock_task_conf.location_map_file_name = fname
    mock_transformer.task_configuration = mock_task_conf

    mock_transformer.folio_keys = ["permanentLocationId"]
    expected = [{"folio_code": "STACKS", "legacy_code": "stacks"}]
    mock_transformer.load_ref_data_mapping_file = Mock(return_value=expected)

    monkeypatch.setattr("os.path.isfile", lambda _: True)
    result = HoldingsCsvTransformer.load_location_map(mock_transformer)
    mock_transformer.load_ref_data_mapping_file.assert_called()
    assert result == expected


def test_load_mapped_fields_reads_file(tmp_path, monkeypatch):
    # Create a holdings map file and ensure the function returns parsed map and calls MappingFileMapperBase
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.folder_structure = Mock()
    mock_transformer.folder_structure.mapping_files_folder = tmp_path

    fname = "holdings_map.json"
    holdings_map = {"data": [{"legacy_field": "lf1", "folio_field": "ff1"}]}
    (tmp_path / fname).write_text(json.dumps(holdings_map))

    mock_task_conf = Mock()
    mock_task_conf.holdings_map_file_name = fname
    mock_transformer.task_configuration = mock_task_conf

    # Patch the MappingFileMapperBase.get_mapped_folio_properties_from_map to return a known value
    from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
        MappingFileMapperBase,
    )

    monkeypatch.setattr(
        MappingFileMapperBase,
        "get_mapped_folio_properties_from_map",
        lambda m: {"mapped": True},
    )

    mapped_fields, parsed_map = HoldingsCsvTransformer.load_mapped_fields(mock_transformer)
    assert mapped_fields == {"mapped": True}
    assert parsed_map == holdings_map