import pymarc

from unittest.mock import Mock

from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)


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
    former_ids = ["formerid123"]

    RulesMapperHoldings.remove_from_id_map(mocked_rules_mapper_holdings, former_ids)

    assert "formerid123" not in mocked_rules_mapper_holdings.holdings_id_map


def test_set_default_call_number_type_if_empty():
    mocked_rules_mapper_holdings = Mock(spec=RulesMapperHoldings)

    folio_holding = {"callNumberTypeId": ""}

    RulesMapperHoldings.set_default_call_number_type_if_empty(
        mocked_rules_mapper_holdings, folio_holding
    )

    assert folio_holding["callNumberTypeId"]
