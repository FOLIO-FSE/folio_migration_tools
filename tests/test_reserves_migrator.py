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


class _Reserve:
    def __init__(self, item_barcode):
        self.item_barcode = item_barcode


def _make_migrator(reserves, skip=False):
    from unittest.mock import Mock

    m = Mock(spec=ReservesMigrator)
    m.semi_valid_reserves = reserves
    m.skip_barcode_prevalidation = skip
    m.migration_report = Mock()
    m.folio_client = Mock()
    m.check_barcodes = lambda: ReservesMigrator.check_barcodes(m)
    return m


def test_check_barcodes_keeps_only_reserves_with_folio_items():
    good, bad = _Reserve("I1"), _Reserve("I2")
    m = _make_migrator([good, bad])
    m.folio_client.folio_post.return_value = {"items": [{"barcode": "I1"}]}

    assert list(ReservesMigrator.check_barcodes(m)) == [good]


def test_prevalidation_can_be_skipped():
    reserves = [_Reserve("I1")]
    m = _make_migrator(reserves, skip=True)

    ReservesMigrator._pre_validate_barcodes(m)

    assert m.valid_reserves == reserves
    m.folio_client.folio_post.assert_not_called()


@pytest.mark.asyncio
async def test_do_work_posts_only_prevalidated_reserves():
    good, bad = _Reserve("I1"), _Reserve("I2")
    m = _make_migrator([good, bad])
    m.folio_client.folio_post.return_value = {"items": [{"barcode": "I1"}]}
    m.t0 = 0
    m._pre_validate_barcodes = lambda: ReservesMigrator._pre_validate_barcodes(m)

    await ReservesMigrator.do_work(m)

    m.post_single_reserve.assert_called_once_with(good)
