import json
import logging
from functools import partial
from unittest.mock import Mock

from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from .test_infrastructure import mocked_classes
from folio_uuid.folio_namespaces import FOLIONamespaces

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True

FILE_DEF = FileDefinition(
    file_name="holdings.tsv", discovery_suppressed=False, staff_suppressed=False
)


def mocked_transformer() -> Mock:
    """A transformer stand-in with the methods post_process_holding calls back into."""
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_mapper.base_string_for_folio_uuid = "test_tenant"
    mock_mapper.schema = {}
    mock_mapper.task_configuration = Mock()
    # The writer caches in a class attribute shared by all of its instances
    mock_mapper.extradata_writer.cache.clear()
    for name in [
        "create_bound_with_holdings",
        "create_and_write_boundwith_part",
        "generate_boundwith_holding_uuid",
        "get_id_map_tuple",
    ]:
        setattr(mock_mapper, name, partial(getattr(MapperBase, name), mock_mapper))

    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper
    mock_transformer.folio_client = mock_mapper.folio_client
    mock_transformer.object_type = FOLIONamespaces.holdings
    mock_transformer.holdings = {}
    mock_transformer.holdings_id_map = {}
    mock_transformer.bound_with_keys = set()
    mock_transformer.legacy_id_to_holdings_key = {}
    mock_transformer.fallback_holdings_type = {"id": "fallback_holdings_type_id"}
    mock_transformer.task_configuration = Mock()
    mock_transformer.task_configuration.holdings_type_uuid_for_boundwiths = "bw_holdings_type_id"
    mock_transformer.task_configuration.holdings_merge_criteria = [
        "instanceId",
        "permanentLocationId",
    ]
    for name in [
        "post_process_holding",
        "create_bound_with_holdings",
        "merge_holding_in",
        "merge_holding",
        "populate_holdings_id_map",
    ]:
        setattr(
            mock_transformer,
            name,
            partial(getattr(HoldingsCsvTransformer, name), mock_transformer),
        )
    return mock_transformer


def holdings_row(legacy_id: str, instance_ids: list[str], **overrides) -> dict:
    return {
        "id": f"holdings_uuid_for_{legacy_id}",
        "formerIds": [legacy_id],
        "instanceId": instance_ids,
        "permanentLocationId": "loc_1",
        "holdingsTypeId": "",
    } | overrides


def test_get_object_type():
    assert HoldingsCsvTransformer.get_object_type() == FOLIONamespaces.holdings


def test_generate_boundwith_part(caplog):
    mock_mapper = mocked_classes.mocked_holdings_mapper()
    mock_transformer = Mock(spec=HoldingsCsvTransformer)
    mock_transformer.mapper = mock_mapper

    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_mapper.base_string_for_folio_uuid = "test_tenant"  # Use tenant_id as base string
    HoldingsMapper.create_and_write_boundwith_part(mock_mapper, "legacy_id", "holding_uuid")

    assert any("boundwithPart\t" in ed for ed in mock_mapper.extradata_writer.cache)
    assert any(
        '"itemId": "c6792640-a656-527f-84e7-e2524c141f66"' in ed
        for ed in mock_mapper.extradata_writer.cache
    )
    assert any(
        '"id": "f5411afb-a2a3-5ce3-9e59-16a67c573bda"' in ed
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


def test_id_map_for_regular_holdings():
    mock_transformer = mocked_transformer()

    mock_transformer.post_process_holding(
        holdings_row("item_1", ["Instance_1"]), "item_1", FILE_DEF
    )
    mock_transformer.post_process_holding(
        holdings_row("item_2", ["Instance_2"]), "item_2", FILE_DEF
    )
    # Merges into the holdings record created from the first row
    mock_transformer.post_process_holding(
        holdings_row("item_3", ["Instance_1"]), "item_3", FILE_DEF
    )
    mock_transformer.populate_holdings_id_map()

    assert len(mock_transformer.holdings) == 2
    id_map = mock_transformer.holdings_id_map
    assert id_map["item_1"][1] == "holdings_uuid_for_item_1"
    assert id_map["item_2"][1] == "holdings_uuid_for_item_2"
    assert id_map["item_3"][1] == "holdings_uuid_for_item_1"


def test_id_map_for_boundwith_row_points_at_first_copy():
    """The item goes on the copy whose UUID was derived from the item's own legacy id."""
    mock_transformer = mocked_transformer()
    row = holdings_row("item_1", ["Instance_1", "Instance_2", "Instance_3"])

    mock_transformer.post_process_holding(row, "item_1", FILE_DEF)
    mock_transformer.populate_holdings_id_map()

    assert len(mock_transformer.holdings) == 3
    assert mock_transformer.holdings_id_map["item_1"][1] == "holdings_uuid_for_item_1"
    # All copies carry the legacy id, but only one of them owns the item link
    assert all(
        holding["formerIds"] == ["item_1"] for holding in mock_transformer.holdings.values()
    )
    # The item is still discoverable from every instance in the set
    boundwith_parts = [
        json.loads(ed.split("\t", 1)[1])
        for ed in mock_transformer.mapper.extradata_writer.cache
        if ed.startswith("boundwithPart\t")
    ]
    assert {part["holdingsRecordId"] for part in boundwith_parts} == {
        holding["id"] for holding in mock_transformer.holdings.values()
    }


def test_id_map_for_two_items_in_the_same_boundwith_set():
    mock_transformer = mocked_transformer()
    instance_ids = ["Instance_1", "Instance_2"]

    mock_transformer.post_process_holding(holdings_row("item_1", instance_ids), "item_1", FILE_DEF)
    mock_transformer.post_process_holding(holdings_row("item_2", instance_ids), "item_2", FILE_DEF)
    mock_transformer.populate_holdings_id_map()

    assert len(mock_transformer.holdings) == 2
    id_map = mock_transformer.holdings_id_map
    # The second row's holdings were merged into the first row's, so both items end up on the
    # copy tied to the first instance of the set.
    assert id_map["item_1"][1] == "holdings_uuid_for_item_1"
    assert id_map["item_2"][1] == "holdings_uuid_for_item_1"


def test_id_map_covers_legacy_ids_only_present_in_former_ids():
    """Legacy bib ids exploded out of a boundwith row are referenced directly by items."""
    mock_transformer = mocked_transformer()
    row = holdings_row("item_1", ["Instance_1"], formerIds=["item_1", "bib_1"])

    mock_transformer.post_process_holding(row, "item_1", FILE_DEF)
    mock_transformer.populate_holdings_id_map()

    assert mock_transformer.holdings_id_map["bib_1"][1] == "holdings_uuid_for_item_1"
    assert mock_transformer.holdings_id_map["item_1"][1] == "holdings_uuid_for_item_1"
