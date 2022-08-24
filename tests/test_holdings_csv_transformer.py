import logging
from unittest.mock import Mock

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_get_object_type():
    assert HoldingsCsvTransformer.get_object_type() == FOLIONamespaces.holdings


def test_generate_boundwith_part(caplog):
    caplog.set_level(25)
    mock_folio = mocked_classes.mocked_folio_client()
    HoldingsCsvTransformer.generate_boundwith_part(mock_folio, "legacy_id", {"id": "holding_uuid"})
    assert "Level 25" in caplog.text
    assert "boundwithPart\t" in caplog.text
    assert '"itemId": "02b904dc-b824-55ac-8e56-e50e395f18f8"}\n' in caplog.text
    assert '"holdingsRecordId": "holding_uuid"' in caplog.text


def test_merge_holding_in_first_boundwith(caplog):

    mock_folio = mocked_classes.mocked_folio_client()

    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.bound_with_keys = set()
    mock_transformer.holdings = {}
    mock_transformer.folio_client = mock_folio
    mock_transformer.mapper = mock_mapper

    new_holding = {"instanceId": "Instance_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)
    assert len(mock_transformer.holdings) == 1
    assert "bw_Instance_1_Instance_1_Instance_2" in mock_transformer.bound_with_keys


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

    new_holding = {"instanceId": "Instance_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)

    new_holding_2 = {"instanceId": "Instance_1"}
    instance_ids_2: list[str] = ["Instance_1", "Instance_2"]
    item_id_2: str = "item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, new_holding_2, instance_ids_2, item_id_2
    )
    assert len(mock_transformer.holdings) == 1
    assert len(mock_transformer.bound_with_keys) == 1
    assert "bw_Instance_1_Instance_1_Instance_2" in mock_transformer.bound_with_keys


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

    new_holding = {"instanceId": "Instance_1"}
    instance_ids: list[str] = ["Instance_1", "Instance_2"]
    item_id: str = "item_1"

    HoldingsCsvTransformer.merge_holding_in(mock_transformer, new_holding, instance_ids, item_id)

    new_holding_2 = {"instanceId": "Instance_2"}
    instance_ids_2: list[str] = ["Instance_3", "Instance_2"]
    item_id_2: str = "item_2"

    HoldingsCsvTransformer.merge_holding_in(
        mock_transformer, new_holding_2, instance_ids_2, item_id_2
    )
    assert len(mock_transformer.holdings) == 2
    assert len(mock_transformer.bound_with_keys) == 2
    assert "bw_Instance_2_Instance_2_Instance_3" in mock_transformer.bound_with_keys
