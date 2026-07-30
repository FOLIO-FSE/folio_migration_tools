from unittest.mock import AsyncMock, Mock, PropertyMock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.migration_tasks.requests_migrator import RequestsMigrator
from .test_infrastructure import mocked_classes


def test_get_object_type():
    assert RequestsMigrator.get_object_type() == FOLIONamespaces.requests


class DummyLegacyRequest:
    def __init__(
        self,
        item_barcode: str = "item-1",
        patron_barcode: str = "patron-1",
        pickup_servicepoint_id: str = "",
    ):
        self.item_barcode = item_barcode
        self.patron_barcode = patron_barcode
        self.pickup_servicepoint_id = pickup_servicepoint_id
        self.request_date = 1

    def to_source_dict(self):
        return {
            "item_barcode": self.item_barcode,
            "patron_barcode": self.patron_barcode,
        }


class TestNormalizeIdentifierFields:
    def test_handles_string(self):
        result = RequestsMigrator._normalize_identifier_fields("barcode, externalSystemId")

        assert result == ["barcode", "externalSystemId"]

    def test_handles_nested_collections(self):
        result = RequestsMigrator._normalize_identifier_fields(
            {
                "prefPatronIdentifier": ["barcode", "identifiers[0].value"],
                "fallback": "username",
            }
        )

        assert result == ["barcode", "identifiers[0].value", "username"]


class TestPreValidatePatronBarcodesAsync:
    def _make_migrator(self, requests, patron_identifiers=None):
        m = Mock(spec=RequestsMigrator)
        m.semi_valid_legacy_requests = requests
        m.patron_identifiers = patron_identifiers or ["barcode", "externalSystemId"]
        m.folio_client = Mock()
        m.folio_client.folio_get_async = AsyncMock()
        m.valid_patron_map = {}
        m._flatten_identifier_values = RequestsMigrator._flatten_identifier_values
        m._get_patron_lookup_value = RequestsMigrator._get_patron_lookup_value.__get__(
            m, RequestsMigrator
        )
        return m

    @pytest.mark.asyncio
    async def test_valid_patron_found(self):
        requests = [DummyLegacyRequest(patron_barcode="P001")]
        m = self._make_migrator(requests)
        m.folio_client.folio_get_async.return_value = [{"barcode": "P001", "id": "u-1"}]

        await RequestsMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_uses_nested_identifier_value_lookup(self):
        requests = [DummyLegacyRequest(patron_barcode="P001")]
        m = self._make_migrator(requests, patron_identifiers=["identifiers[0].value"])
        m.folio_client.folio_get_async.return_value = [
            {"id": "u-1", "identifiers": [{"value": "P001"}]}
        ]

        await RequestsMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_no_patron_found(self):
        requests = [DummyLegacyRequest(patron_barcode="P002")]
        m = self._make_migrator(requests)
        m.folio_client.folio_get_async.return_value = []

        await RequestsMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {}


class TestPreValidateItemBarcodes:
    def _make_migrator(self, requests):
        m = Mock(spec=RequestsMigrator)
        m.semi_valid_legacy_requests = requests
        m.folio_client = Mock()
        m.valid_item_barcodes = set()
        return m

    def test_batches_and_collects_item_barcodes(self):
        requests = [DummyLegacyRequest(item_barcode=f"I{i:04d}") for i in range(4)]
        m = self._make_migrator(requests)
        m.folio_client.folio_post.side_effect = [
            {
                "items": [
                    {"barcode": "I0000", "id": "item-0"},
                    {"barcode": "I0001", "id": "item-1"},
                ]
            },
            {
                "items": [
                    {"barcode": "I0002", "id": "item-2"},
                    {"barcode": "I0003", "id": "item-3"},
                ]
            },
        ]

        RequestsMigrator.pre_validate_item_barcodes(m, batch_size=2)

        assert m.folio_client.folio_post.call_count == 2
        assert m.valid_item_barcodes == {"I0000", "I0001", "I0002", "I0003"}


class TestCheckBarcodes:
    def _make_migrator(self, requests):
        m = Mock(spec=RequestsMigrator)
        m.semi_valid_legacy_requests = requests
        m.failed_requests = set()
        m.migration_report = Mock()
        m.valid_item_barcodes = set()
        m.valid_patron_map = {}
        m.pre_validate_item_barcodes = Mock()
        m.pre_validate_patron_barcodes_async = AsyncMock()
        return m

    @pytest.mark.asyncio
    async def test_yields_request_when_barcodes_are_valid(self):
        req = DummyLegacyRequest(item_barcode="I001", patron_barcode="OLDP")
        m = self._make_migrator([req])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"OLDP": "NEWP"}

        result = []
        async for request in RequestsMigrator.check_barcodes(m):
            result.append(request)

        assert result == [req]
        assert req.patron_barcode == "NEWP"

    @pytest.mark.asyncio
    async def test_discards_request_when_item_or_patron_is_invalid(self):
        req = DummyLegacyRequest(item_barcode="I001", patron_barcode="P001")
        m = self._make_migrator([req])
        m.valid_item_barcodes = set()
        m.valid_patron_map = {}

        result = []
        async for request in RequestsMigrator.check_barcodes(m):
            result.append(request)

        assert result == []
        assert req in m.failed_requests


class TestPreValidateBarcodesWorkflow:
    @pytest.mark.asyncio
    async def test_skip_prevalidation_uses_source_requests(self):
        req = DummyLegacyRequest()
        m = Mock(spec=RequestsMigrator)
        m.task_configuration = Mock()
        m.task_configuration.skip_barcode_prevalidation = True
        m.semi_valid_legacy_requests = [req]
        m.check_barcodes = AsyncMock()

        await RequestsMigrator._pre_validate_barcodes(m)

        assert m.valid_legacy_requests == [req]

    @pytest.mark.asyncio
    async def test_prevalidation_uses_async_check_barcodes(self):
        req1 = DummyLegacyRequest(item_barcode="I001", patron_barcode="P001")
        req2 = DummyLegacyRequest(item_barcode="I002", patron_barcode="P002")
        m = Mock(spec=RequestsMigrator)
        m.task_configuration = Mock()
        m.task_configuration.skip_barcode_prevalidation = False
        m.semi_valid_legacy_requests = [req1, req2]

        async def _gen():
            yield req1

        m.check_barcodes = _gen

        await RequestsMigrator._pre_validate_barcodes(m)

        assert m.valid_legacy_requests == [req1]


class TestPreValidateServicePoints:
    def _make_migrator(self, requests, folio_service_points=None):
        m = Mock(spec=RequestsMigrator)
        m.semi_valid_legacy_requests = requests
        m.folio_client = Mock()
        m.folio_client.service_points = folio_service_points or []
        return m

    @pytest.mark.asyncio
    async def test_resolves_code_to_uuid(self):
        req = DummyLegacyRequest(pickup_servicepoint_id="circ-desk")
        folio_sps = [{"id": "sp-uuid-1", "code": "circ-desk"}]
        m = self._make_migrator([req], folio_sps)

        await RequestsMigrator.pre_validate_service_points(m)

        assert req.pickup_servicepoint_id == "sp-uuid-1"

    @pytest.mark.asyncio
    async def test_uuid_passes_through_unchanged(self):
        req = DummyLegacyRequest(pickup_servicepoint_id="sp-uuid-1")
        folio_sps = [{"id": "sp-uuid-1", "code": "circ-desk"}]
        m = self._make_migrator([req], folio_sps)

        await RequestsMigrator.pre_validate_service_points(m)

        assert req.pickup_servicepoint_id == "sp-uuid-1"

    @pytest.mark.asyncio
    async def test_exits_on_missing_uuid(self):
        req = DummyLegacyRequest(
            pickup_servicepoint_id="00000000-0000-0000-0000-000000000099"
        )
        folio_sps = [{"id": "sp-uuid-1", "code": "circ-desk"}]
        m = self._make_migrator([req], folio_sps)

        with pytest.raises(SystemExit):
            await RequestsMigrator.pre_validate_service_points(m)

    @pytest.mark.asyncio
    async def test_exits_on_missing_code(self):
        req = DummyLegacyRequest(pickup_servicepoint_id="bad-code")
        folio_sps = [{"id": "sp-uuid-1", "code": "circ-desk"}]
        m = self._make_migrator([req], folio_sps)

        with pytest.raises(SystemExit):
            await RequestsMigrator.pre_validate_service_points(m)

    @pytest.mark.asyncio
    async def test_no_requests_returns_early(self):
        m = Mock(spec=RequestsMigrator)
        m.semi_valid_legacy_requests = []
        m.folio_client = Mock()
        sp_mock = PropertyMock()
        type(m.folio_client).service_points = sp_mock

        await RequestsMigrator.pre_validate_service_points(m)

        sp_mock.assert_not_called()


class TestServicePointMappingInit:
    """Test _init_service_point_mapping using real RefDataMapping and mocked FolioClient."""

    def test_creates_ref_data_mapping_from_tsv_file(self, tmp_path):
        """Integration: loads a real TSV mapping file and produces a RefDataMapping."""
        import csv

        csv.register_dialect("tsv", delimiter="\t")
        # Create a TSV mapping file with legacy_code -> folio_code
        map_file = tmp_path / "sp_map.tsv"
        map_file.write_text("service_point_id\tfolio_code\nold_desk\tlmd\n*\tfo\n")

        m = Mock(spec=RequestsMigrator)
        m.folio_client = mocked_classes.mocked_folio_client()
        m.folder_structure = Mock()
        m.folder_structure.mapping_files_folder = tmp_path
        m.load_ref_data_mapping_file = RequestsMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = "sp_map.tsv"

        RequestsMigrator._init_service_point_mapping(m, task_config)

        assert isinstance(m.service_point_mapping, RefDataMapping)
        assert m.service_point_mapping.default_id == "finance_office_uuid"
        assert m.service_point_mapping.regular_mappings[0]["folio_id"] == "library_main_desk_uuid"

    def test_skips_loading_when_no_map_file_configured(self):
        m = Mock(spec=RequestsMigrator)
        m.load_ref_data_mapping_file = RequestsMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = ""

        RequestsMigrator._init_service_point_mapping(m, task_config)

        assert m.service_point_mapping is None

    def test_sets_none_when_map_file_does_not_exist(self, tmp_path):
        """When the configured file doesn't exist, service_point_mapping stays None."""
        m = Mock(spec=RequestsMigrator)
        m.folio_client = mocked_classes.mocked_folio_client()
        m.folder_structure = Mock()
        m.folder_structure.mapping_files_folder = tmp_path
        m.load_ref_data_mapping_file = RequestsMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = "nonexistent.tsv"

        RequestsMigrator._init_service_point_mapping(m, task_config)

        assert m.service_point_mapping is None
