from unittest.mock import Mock

import pymarc
from pymarc import Field
from pymarc import Record

from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)


def test_get_legacy_ids_001():
    record = Record()
    mock_mapper = Mock(spec=RulesMapperHoldings)
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "001"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config
    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["0001"]


def test_get_legacy_id_001_wrong_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=["b", "bid"],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=["c", "cid"],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "951$c"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config

    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["cid"]


def test_get_legacy_id_001_right_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=["c", "cid"],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=["b", "bid"],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "951$c"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config
    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["cid"]


def test_get_holdings_schema():
    schema = RulesMapperHoldings.fetch_holdings_schema()
    assert schema["required"]


def test_get_marc_textual_stmt_correct_order_and_not_deduped():
    path = "./tests/test_data/mfhd/c_record_repeated_holdings_statements.mrc"
    with open(path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record = next(reader)
        res = HoldingsStatementsParser.get_holdings_statements(
            record, "853", "863", "866", ["apa"], False
        )
        rec = {"holdingsStatements": res["statements"]}
        props_to_not_dedupe = [
            "holdingsStatements",
            "holdingsStatementsForIndexes",
            "holdingsStatementsForSupplements",
        ]
        RulesMapperHoldings.dedupe_rec(rec, props_to_not_dedupe)
        stmt = "1994-1998."
        all_stmts = [f["statement"] for f in rec["holdingsStatements"]]
        all_94s = [f for f in all_stmts if stmt == f]
        assert len(all_94s) == 2


def test_remove_from_id_map():
    mocked_rules_mapper_holdings = Mock(spec=RulesMapperHoldings)
    mocked_rules_mapper_holdings.id_map = {
        "h15066915": "5a0af31f-aa4a-5215-8a60-712b38cd6cb6",
        "h14554914": "c9c44650-11e2-5534-ae50-01a1aa0fbd66",
    }

    former_ids = ["h15066915"]

    RulesMapperHoldings.remove_from_id_map(mocked_rules_mapper_holdings, former_ids)

    # The ids in the former_ids have been removed, any others are still there
    assert "h15066915" not in mocked_rules_mapper_holdings.id_map.keys()
    assert "h14554914" in mocked_rules_mapper_holdings.id_map.keys()


def test_set_default_call_number_type_if_empty():
    mocked_rules_mapper_holdings = Mock(spec=RulesMapperHoldings)
    mocked_rules_mapper_holdings.conditions = Mock(spec=Conditions)
    mocked_rules_mapper_holdings.conditions.default_call_number_type = {
        "id": "b8992e1e-1757-529f-9238-147703864635"
    }

    without_callno_type_specified = {"callNumberTypeId": ""}
    with_callno_type_specified = {"callNumberTypeId": "22156b02-785f-51c0-8723-416512cd42d9"}

    # The default callNumberTypeId is assigned when no callNumberTypeId is specified
    RulesMapperHoldings.set_default_call_number_type_if_empty(
        mocked_rules_mapper_holdings, without_callno_type_specified
    )
    assert (
        without_callno_type_specified["callNumberTypeId"] == "b8992e1e-1757-529f-9238-147703864635"
    )

    # The default callNumberTypeId is not replaced with the default if one is already specified
    RulesMapperHoldings.set_default_call_number_type_if_empty(
        mocked_rules_mapper_holdings, with_callno_type_specified
    )
    assert with_callno_type_specified["callNumberTypeId"] == "22156b02-785f-51c0-8723-416512cd42d9"
