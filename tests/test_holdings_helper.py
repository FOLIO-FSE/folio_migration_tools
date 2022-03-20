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
            {"statement": "stmt2", "note": "stmt2", "staffNote": True},
            {"uri": "1", "linkText": "1", "publicNote": "1", "relationshipId": "1"},
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
    assert len(merged_holding["holdingsStatementsForSupplements"]) == 2
    assert len(merged_holding["holdingsStatements"]) == 2


def test_merge_holding2():
    h1 = {
        "metadata": {
            "createdDate": "2022-03-06T21:04:32.890",
            "createdByUserId": "a5004081-7155-48ee-8edf-77b09eec9980",
            "updatedDate": "2022-03-06T21:04:32.890",
            "updatedByUserId": "a5004081-7155-48ee-8edf-77b09eec9980",
        },
        "receiptStatus": "Unknown",
        "instanceId": "1",
        "formerIds": [".c10418477", "1"],
        "permanentLocationId": "1130ebb7-bbeb-4166-93aa-a57ec8876b71",
        "copyNumber": "1",
        "notes": [
            {
                "note": "copy 1",
                "holdingsNoteTypeId": "124becfb-bceb-4623-a9d3-2a439ac3fcf5",
                "staffOnly": True,
            }
        ],
        "id": "28d38a8c-5401-50b8-89e5-33e78e572e91",
        "holdingsTypeId": "e6da6c98-6dd0-41bc-8b4b-cfd4bbd9c3ae",
        "callNumberTypeId": "95467209-6d7b-468b-94df-0f5d7ad2747d",
        "holdingsStatements": [
            {
                "statement": "1:1,1:3-1:6,2:1-2:2,2:5,2:10-11:1,11:3-11:7,12:3-12:5,12:7,12:11; 18:4-20:4,20:6,22:4-22:6,23:1-23:2,23:4,26:3,26:5-26:6,27:1,27:3-27:4,31:2-31:6, 32:1-32:2 (1976-1987;1993-1998;2001-2002,2007)",
                "note": "",
                "staffNote": "",
            }
        ],
        "holdingsStatementsForIndexes": [
            {
                "statement": "stmt1",
                "note": "",
                "staffNote": "",
            }
        ],
        "holdingsStatementsForSupplements": [],
        "discoverySuppress": False,
        "sourceId": "036ee84a-6afd-4c3c-9ad3-4a12ab875f59",
        "hrid": "ho00900565900",
    }
    h2 = {
        "metadata": {
            "createdDate": "2022-03-06T21:04:32.890",
            "createdByUserId": "a5004081-7155-48ee-8edf-77b09eec9980",
            "updatedDate": "2022-03-06T21:04:32.890",
            "updatedByUserId": "a5004081-7155-48ee-8edf-77b09eec9980",
        },
        "receiptStatus": "Unknown",
        "instanceId": "96a552a9-58ed-5472-9b29-c8cf26d30e7c",
        "formerIds": ["2", "i1"],
        "permanentLocationId": "1130ebb7-bbeb-4166-93aa-a57ec8876b71",
        "copyNumber": "1",
        "notes": [
            {
                "note": "copy 1",
                "holdingsNoteTypeId": "124becfb-bceb-4623-a9d3-2a439ac3fcf5",
                "staffOnly": True,
            }
        ],
        "id": "2",
        "holdingsTypeId": "e6da6c98-6dd0-41bc-8b4b-cfd4bbd9c3ae",
        "callNumberTypeId": "95467209-6d7b-468b-94df-0f5d7ad2747d",
        "holdingsStatements": [],
        "holdingsStatementsForIndexes": [
            {
                "statement": "stmt2",
                "note": "",
                "staffNote": "",
            }
        ],
        "holdingsStatementsForSupplements": [],
        "discoverySuppress": False,
        "sourceId": "036ee84a-6afd-4c3c-9ad3-4a12ab875f59",
        "hrid": "ho00900565900",
    }
    merged_holding = HoldingsHelper.merge_holding(h1, h2)
    assert len(merged_holding["holdingsStatements"]) == 1
    assert len(merged_holding["holdingsStatementsForIndexes"]) == 2
    assert len(merged_holding["formerIds"]) == 4


def test_holdings_notes():
    folio_rec = {"notes": [{"note": "apa", "holdingsNoteTypeId": ""}]}
    with pytest.raises(TransformationProcessError):
        HoldingsHelper.handle_notes(folio_rec)


def test_holdings_notes2():
    folio_rec = {"notes": [{"note": "", "holdingsNoteTypeId": "apa"}]}
    HoldingsHelper.handle_notes(folio_rec)
    assert "notes" not in folio_rec
