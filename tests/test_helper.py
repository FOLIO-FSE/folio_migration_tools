import logging

import pytest
from folioclient import FolioClient
from requests.exceptions import HTTPError

from folio_migration_tools.helper import Helper

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


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


def test_log_dats_issue(caplog):
    caplog.set_level(26)
    Helper.log_data_issue("test id", "test log message", "legacy value")
    assert "test log message" in caplog.text
    assert "test id" in caplog.text
    assert "legacy value" in caplog.text
    # logging.log(26, "DATA ISSUE\t%s\t%s\t%s", index_or_id, message, legacy_value)
