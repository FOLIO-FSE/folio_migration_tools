from unittest.mock import AsyncMock, Mock

import pytest

import folioclient

from folio_migration_tools.circulation_prevalidation import (
    flatten_identifier_values,
    get_patron_lookup_value,
    load_patron_identifiers,
    normalize_identifier_fields,
    validate_item_barcodes,
    validate_patron_barcodes,
)


class TestNormalizeIdentifierFields:
    def test_handles_string(self):
        assert normalize_identifier_fields("barcode, externalSystemId") == [
            "barcode",
            "externalSystemId",
        ]

    def test_handles_nested_collections(self):
        result = normalize_identifier_fields(
            {
                "prefPatronIdentifier": ["barcode", "identifiers[0].value"],
                "fallback": "username",
            }
        )

        assert result == ["barcode", "identifiers[0].value", "username"]


class TestLoadPatronIdentifiers:
    def test_reads_comma_separated_setting(self):
        client = Mock()
        client.folio_get_single_object.return_value = {
            "configs": [{"value": '{"prefPatronIdentifier": "barcode,username"}'}]
        }

        assert load_patron_identifiers(client) == ["barcode", "username"]

    def test_defaults_to_barcode_when_missing(self):
        client = Mock()
        client.folio_get_single_object.return_value = {}

        assert load_patron_identifiers(client) == ["barcode"]


class TestValidatePatronBarcodes:
    def _client(self, response):
        client = Mock()
        client.folio_get_async = AsyncMock(return_value=response)
        return client

    @pytest.mark.asyncio
    async def test_valid_patron_found(self):
        client = self._client([{"barcode": "P001", "id": "u-1", "patronGroup": "g"}])

        result = await validate_patron_barcodes(client, {"P001"}, ["barcode"])

        assert result == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_uses_nested_identifier_value_lookup(self):
        client = self._client(
            [{"id": "u-1", "patronGroup": "g", "identifiers": [{"value": "P001"}]}]
        )

        result = await validate_patron_barcodes(client, {"P001"}, ["identifiers[0].value"])

        assert result == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_no_patron_found(self):
        result = await validate_patron_barcodes(self._client([]), {"P002"}, ["barcode"])

        assert result == {}

    @pytest.mark.asyncio
    async def test_multiple_patrons_rejected(self):
        patrons = [{"barcode": "P1", "patronGroup": "g"}, {"barcode": "P1", "patronGroup": "g"}]

        result = await validate_patron_barcodes(self._client(patrons), {"P1"}, ["barcode"])

        assert result == {}

    @pytest.mark.asyncio
    async def test_patron_without_group_rejected(self):
        client = self._client([{"barcode": "P1", "patronGroup": ""}])

        assert await validate_patron_barcodes(client, {"P1"}, ["barcode"]) == {}

    @pytest.mark.asyncio
    async def test_require_barcode_rejects_patron_matched_by_other_identifier(self):
        client = self._client([{"id": "u-1", "patronGroup": "g", "username": "jdoe"}])

        result = await validate_patron_barcodes(
            client, {"jdoe"}, ["username"], require_barcode=True
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_require_barcode_returns_folio_barcode(self):
        client = self._client([{"barcode": "NEW", "patronGroup": "g", "username": "jdoe"}])

        result = await validate_patron_barcodes(
            client, {"jdoe"}, ["username"], require_barcode=True
        )

        assert result == {"jdoe": "NEW"}


class TestValidateItemBarcodes:
    def test_batches_and_collects_item_barcodes(self):
        client = Mock()
        client.folio_post.side_effect = [
            {"items": [{"barcode": "I0"}, {"barcode": "I1"}]},
            {"items": [{"barcode": "I2"}]},
        ]

        result = validate_item_barcodes(client, {"I0", "I1", "I2", "I3"}, batch_size=2)

        assert client.folio_post.call_count == 2
        assert result <= {"I0", "I1", "I2"}
        assert "I3" not in result

    def test_non_dict_response_yields_no_valid_barcodes(self):
        client = Mock()
        client.folio_post.return_value = []

        assert validate_item_barcodes(client, {"I1"}) == set()


class TestFlattenIdentifierValues:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, []),
            ("  abc ", ["abc"]),
            ("   ", []),
            (123, ["123"]),
            (["a", ["b", None]], ["a", "b"]),
            ({"value": "v", "other": "x"}, ["v"]),
            ({"unknown": "u", "nested": {"deep": "d"}}, ["u", "d"]),
            (object(), []),
        ],
    )
    def test_flatten(self, value, expected):
        assert flatten_identifier_values(value) == expected


class TestGetPatronLookupValue:
    def test_checks_barcode_field_before_configured_identifiers(self):
        patron = {"barcode": "NEW", "username": "OLD"}

        assert get_patron_lookup_value(patron, "OLD", ["username"]) == "NEW"
        assert get_patron_lookup_value({"username": "OLD"}, "OLD", ["username"]) == "OLD"

    def test_falls_back_to_first_value(self):
        assert get_patron_lookup_value({"barcode": "NEW"}, "OLD", ["barcode"]) == "NEW"

    def test_uses_flat_key_when_path_lookup_misses(self):
        assert get_patron_lookup_value({"a.b": "X"}, "X", ["a.b"]) == "X"

    def test_returns_none_when_no_identifier_resolves(self):
        assert get_patron_lookup_value({"id": "u"}, "OLD", ["username"]) is None


class TestLoadPatronIdentifiersErrors:
    def test_falls_back_on_invalid_json(self):
        client = Mock()
        client.folio_get_single_object.return_value = {"configs": [{"value": "not json"}]}

        assert load_patron_identifiers(client) == ["barcode"]

    def test_falls_back_on_client_error_with_response(self):
        client = Mock()
        error = folioclient.FolioClientError(
            "boom", request=Mock(), response=Mock(text="server said no")
        )
        client.folio_get_single_object.side_effect = error

        assert load_patron_identifiers(client) == ["barcode"]


class TestFetchErrors:
    @pytest.mark.asyncio
    async def test_fetch_error_marks_patron_invalid(self):
        client = Mock()
        client.folio_get_async = AsyncMock(side_effect=RuntimeError("down"))

        assert await validate_patron_barcodes(client, {"P1"}, ["barcode"]) == {}

    @pytest.mark.asyncio
    async def test_fetch_error_with_response_text(self):
        client = Mock()
        error = RuntimeError("down")
        error.response = Mock(text="gateway timeout")
        client.folio_get_async = AsyncMock(side_effect=error)

        assert await validate_patron_barcodes(client, {"P1"}, ["barcode"]) == {}

    @pytest.mark.asyncio
    async def test_progress_logged_every_hundred(self):
        client = Mock()
        client.folio_get_async = AsyncMock(return_value=[])

        result = await validate_patron_barcodes(client, {f"P{i}" for i in range(100)}, ["barcode"])

        assert result == {}

    def test_item_batch_error_is_logged_and_skipped(self):
        client = Mock()
        error = folioclient.FolioClientError("boom", request=Mock(), response=Mock(text="bad"))
        client.folio_post.side_effect = error

        assert validate_item_barcodes(client, {"I1"}) == set()
