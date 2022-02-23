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
