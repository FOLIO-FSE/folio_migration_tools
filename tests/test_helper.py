from requests.exceptions import HTTPError
import pytest
from folioclient import FolioClient


def test_get_latest_from_github_returns_none_when_failing():
    with pytest.raises(HTTPError):
        FolioClient.get_latest_from_github("branchedelac", "tati", "myfile.json")


def test_get_latest_from_github_returns_file_1():
    schema = FolioClient.get_latest_from_github(
        "folio-org",
        "mod-source-record-manager",
        "/mod-source-record-manager-server/src/main/resources/rules/marc_holdings_rules.json",
    )
    assert schema is not None
    assert schema.get("001", None) is not None


def test_get_latest_from_github_returns_file_orgs_has_no_releases():
    with pytest.raises(HTTPError):
        FolioClient.get_latest_from_github(
            "folio-org",
            "acq-models",
            "/mod-orgs/schemas/organization.json",
        )
