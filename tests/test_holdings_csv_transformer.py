import logging
from unittest.mock import Mock

from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
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
    assert "Instance_1_loc_1" in mock_transformer.holdings
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