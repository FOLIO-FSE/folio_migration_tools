from unittest.mock import Mock

import pytest
from migration_tools.custom_exceptions import TransformationProcessError
from migration_tools.holdings_helper import HoldingsHelper
from migration_tools.migration_report import MigrationReport
from folioclient import FolioClient


def test_to_key():
    holdings_record = {
        "instanceId": "instance",
        "permanentLocation": "location",
        "callNumber": "callnumber",
    }
    merge_criterias = ["instanceId", "permanentLocation", "callNumber"]
    m = MigrationReport()
    res = HoldingsHelper.to_key(holdings_record, merge_criterias, m)
    assert res == "instance-location-callnumber"


def test_merge_holding():
    holding_1 = dict(
        formerIds=["a", "b"],
        electronicAccess=[{"uri": "2"}],
        holdingsStatementsForSupplements=[
            {"statement": "stmt2", "note": "stmt2", "staffNote": True}
        ],
        holdingsStatements=[{"statement": "stmt3", "note": "stmt3", "staffNote": True}],
    )

    holding_2 = dict(
        formerIds=["c", "d"],
        electronicAccess=[
            {"uri": "1", "linkText": "1", "publicNote": "1", "relationshipId": "1"}
        ],
        holdingsStatementsForIndexes=[
            {"statement": "stmt1", "note": "stmt1", "staffNote": True}
        ],
        holdingsStatements=[{"statement": "stmt4", "note": "stmt4", "staffNote": True}],
    )
    merged_holding = HoldingsHelper.merge_holding(holding_1, holding_2)
    assert sorted(merged_holding["formerIds"]) == ["a", "b", "c", "d"]
    assert len(merged_holding["electronicAccess"]) == 2
    assert len(merged_holding["holdingsStatementsForIndexes"]) == 1
    assert len(merged_holding["holdingsStatementsForSupplements"]) == 1
    assert len(merged_holding["holdingsStatements"]) == 2
