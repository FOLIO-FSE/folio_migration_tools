from unittest.mock import Mock, patch

import httpx
import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.reserves_migrator import ReservesMigrator


def test_get_object_type():
    assert ReservesMigrator.get_object_type() == FOLIONamespaces.reserve


def _response(status_code: int, body: str = '{"errors": [{"message": "bad"}]}'):
    return httpx.Response(status_code, text=body, request=httpx.Request("POST", "http://folio/x"))


class TestFolioPutPost:
    @pytest.fixture
    def migrator(self):
        m = Mock(spec=ReservesMigrator)
        m.folio_client = Mock()
        m.folio_client.gateway_url = "http://folio"
        m.folio_client.okapi_headers = {}
        m.migration_report = Mock()
        return m

    @pytest.mark.parametrize("status", [201, 204])
    def test_success(self, migrator, status):
        with patch("httpx.post", return_value=_response(status, "")):
            assert ReservesMigrator.folio_put_post(migrator, "/x", {}, "POST", "act")

    @pytest.mark.parametrize("status", [422, 500])
    def test_http_error_returns_false(self, migrator, status):
        with patch("httpx.post", return_value=_response(status)):
            assert not ReservesMigrator.folio_put_post(migrator, "/x", {}, "POST", "act")

    def test_connection_error_returns_false(self, migrator):
        with patch("httpx.put", side_effect=httpx.ConnectError("boom")):
            assert not ReservesMigrator.folio_put_post(migrator, "/x", {}, "PUT", "act")
