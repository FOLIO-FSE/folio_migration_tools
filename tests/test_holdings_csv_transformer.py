import logging
from unittest.mock import Mock

import pytest
from folio_uuid import FolioUUID
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
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


def test_create_bound_with_holdings_test_raise(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper
    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.holdings_type_uuid_for_boundwiths = ""
    mock_transformer.task_config = mocked_config
    # mock_folio = mocked_classes.mocked_folio_client()
    with pytest.raises(TransformationProcessError):
        list(HoldingsCsvTransformer.create_bound_with_holdings(mock_transformer, {}, ""))


def test_create_bound_with_holdings_success(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper
    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.holdings_type_uuid_for_boundwiths = "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    mock_transformer.task_config = mocked_config
    mock_folio = mocked_classes.mocked_folio_client()

    folio_holding = {
        "id": FolioUUID(
            mock_folio.okapi_url,
            FOLIONamespaces.holdings,
            1,
        ),
        "formerIds": ["[b1,b2]"],
        "instanceId": ["i1", "i2"],
    }
    mock_folio = mocked_classes.mocked_folio_client()
    mock_folio.holdings_types.append(
        {
            "id": "c92480eb-210d-442e-a6f7-4043fe7f0f25",
            "name": "Bound-with",
            "source": "local",
        }
    )
    mock_transformer.folio_client = mock_folio
    # mock_folio = mocked_classes.mocked_folio_client()
    bws = list(
        HoldingsCsvTransformer.create_bound_with_holdings(mock_transformer, folio_holding, "")
    )
    assert len(bws) == 2
    assert bws[0]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[0]["id"] == "e84e41cd-141e-503b-94fa-d55196446a05"
    assert bws[0]["instanceId"] == "i1"

    assert bws[1]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[1]["id"] == "0ecaa02d-4fe4-5cc1-bb86-c44a5eceaa49"
    assert bws[1]["instanceId"] == "i2"


def test_create_bound_with_holdings_same_call_number(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper
    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.holdings_type_uuid_for_boundwiths = "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    mock_transformer.task_config = mocked_config
    mock_folio = mocked_classes.mocked_folio_client()

    folio_holding = {
        "id": FolioUUID(
            mock_folio.okapi_url,
            FOLIONamespaces.holdings,
            1,
        ),
        "formerIds": ["[b1,b2]"],
        "instanceId": ["i1", "i2"],
        "callNumber": "Single callnumber",
    }
    mock_folio = mocked_classes.mocked_folio_client()
    mock_folio.holdings_types.append(
        {
            "id": "c92480eb-210d-442e-a6f7-4043fe7f0f25",
            "name": "Bound-with",
            "source": "local",
        }
    )
    mock_transformer.folio_client = mock_folio
    # mock_folio = mocked_classes.mocked_folio_client()
    bws = list(
        HoldingsCsvTransformer.create_bound_with_holdings(mock_transformer, folio_holding, "")
    )
    assert len(bws) == 2
    assert bws[0]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[0]["id"] == "e84e41cd-141e-503b-94fa-d55196446a05"
    assert bws[0]["instanceId"] == "i1"
    assert bws[0]["callNumber"] == bws[1]["callNumber"]
    assert bws[0]["callNumber"] == "Single callnumber"

    assert bws[1]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[1]["id"] == "0ecaa02d-4fe4-5cc1-bb86-c44a5eceaa49"
    assert bws[1]["instanceId"] == "i2"


def test_create_bound_with_holdings_different_call_number(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper
    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.holdings_type_uuid_for_boundwiths = "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    mock_transformer.task_config = mocked_config
    mock_folio = mocked_classes.mocked_folio_client()

    folio_holding = {
        "id": FolioUUID(
            mock_folio.okapi_url,
            FOLIONamespaces.holdings,
            1,
        ),
        "formerIds": ["['b1','b2']"],
        "instanceId": ["i1", "i2"],
        "callNumber": "['first', 'second']",
    }
    mock_folio = mocked_classes.mocked_folio_client()
    mock_folio.holdings_types.append(
        {
            "id": "c92480eb-210d-442e-a6f7-4043fe7f0f25",
            "name": "Bound-with",
            "source": "local",
        }
    )
    mock_transformer.folio_client = mock_folio
    # mock_folio = mocked_classes.mocked_folio_client()
    bws = list(
        HoldingsCsvTransformer.create_bound_with_holdings(mock_transformer, folio_holding, "")
    )
    assert len(bws) == 2
    assert bws[0]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[0]["id"] == "e84e41cd-141e-503b-94fa-d55196446a05"
    assert bws[0]["instanceId"] == "i1"
    assert bws[0]["callNumber"] == "first"

    assert bws[1]["holdingsTypeId"] == "c92480eb-210d-442e-a6f7-4043fe7f0f25"
    assert bws[1]["id"] == "0ecaa02d-4fe4-5cc1-bb86-c44a5eceaa49"
    assert bws[1]["instanceId"] == "i2"
    assert bws[1]["callNumber"] == "second"


def test_generate_boundwith_part(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper

    mock_folio = mocked_classes.mocked_folio_client()
    HoldingsCsvTransformer.generate_boundwith_part(
        mock_transformer, mock_folio, "legacy_id", {"id": "holding_uuid"}
    )

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
    mock_transformer.object_type = FOLIONamespaces.holdings
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
