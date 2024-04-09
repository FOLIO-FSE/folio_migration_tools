import logging
from unittest.mock import Mock

from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.test_infrastructure import mocked_classes
from folio_uuid.folio_namespaces import FOLIONamespaces

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_get_object_type():
    assert HoldingsCsvTransformer.get_object_type() == FOLIONamespaces.holdings


def test_generate_boundwith_part(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper

    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    HoldingsMapper.create_and_write_boundwith_part(mock_mapper, "legacy_id", "holding_uuid")

    assert any("boundwithPart\t" in ed for ed in mock_mapper.extradata_writer.cache)
    assert any(
        '"itemId": "02b904dc-b824-55ac-8e56-e50e395f18f8"}\n' in ed
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
