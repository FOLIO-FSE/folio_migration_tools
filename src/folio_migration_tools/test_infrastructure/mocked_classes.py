from unittest.mock import MagicMock
from unittest.mock import Mock

from folioclient import FolioClient


def mocked_folio_client() -> FolioClient:
    mocked_folio = Mock(spec=FolioClient)
    mocked_folio.okapi_url = "okapi_url"
    mocked_folio.tenant_id = "tenant_id"
    mocked_folio.username = "username"
    mocked_folio.folio_get_all = folio_get_all
    mocked_folio.password = "password"  # noqa: S105
    mocked_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
            "items": {"prefix": "pref", "startNumber": "1"},
            "commonRetainLeadingZeroes": True,
        }
    )
    mocked_folio.instance_formats = [
        {
            "code": "test_code_99",
            "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
            "name": "test -- name",
        },
        {
            "code": "ab",
            "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
            "name": "test -- name 2",
        },
    ]
    return mocked_folio


def folio_get_all(ref_data_path, array_name, query, limit):
    return [
        {"name": "Fall 2022", "id": "42093be3-d1e7-4bb6-b2b9-18e153d109b2"},
        {"name": "Summer 2022", "id": "415b14a8-c94c-4aa1-a0a8-d397efae343e"},
    ]


def get_latest_from_github(owner, repo, file_path):
    return FolioClient.get_latest_from_github(owner, repo, file_path, "")
