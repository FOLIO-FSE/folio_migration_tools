from requests.exceptions import HTTPError
import pytest
from migration_tools.helper import Helper


def test_get_latest_from_github_returns_none_when_failing():
    schema = Helper.get_latest_from_github("branchedelac", "tati", "myfile.json")
    assert schema is None


def test_get_latest_from_github_returns_file():
    schema = Helper.get_latest_from_github(
        "folio-org",
        "mod-source-record-manager",
        "/mod-source-record-manager-server/src/main/resources/rules/marc_holdings_rules.json",
    )
    assert schema is not None
    assert schema.get("001", None) is not None


def test_get_latest_from_github_returns_file():
    schema = Helper.get_latest_from_github(
        "folio-org",
        "acq-models",
        "/mod-orgs/schemas/organization.json",
    )
    assert schema is not None
    assert schema.get("description") == "The record of an organization"
