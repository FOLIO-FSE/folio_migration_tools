import asyncio
import copy
import csv
import json
from io import StringIO
from unittest.mock import AsyncMock, Mock, patch
from zoneinfo import ZoneInfo

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.loans_migrator import LoansMigrator
from .test_infrastructure import mocked_classes


def test_get_object_type():
    assert LoansMigrator.get_object_type() == FOLIONamespaces.loans


def test_load_and_validate_legacy_loans_set_in_source():
    with StringIO() as csvfile:
        csvfile.seek(0)
        fieldnames = [
            "item_barcode",
            "patron_barcode",
            "due_date",
            "out_date",
            "renewal_count",
            "next_item_status",
            "service_point_id",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "item_barcode": "i_barcode",
                "patron_barcode": "p_barcode",
                "due_date": "2020-10-12T02:02:02",
                "out_date": "2020-09-12T02:02:02",
                "renewal_count": "1",
                "next_item_status": "",
                "service_point_id": "Set in source data",
            }
        )
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)

        mock_library_conf = Mock(spec=LibraryConfiguration)
        mock_library_conf.gateway_url = "https://okapi_url"
        mock_library_conf.tenant_id = ""
        mock_library_conf.folio_username = ""
        mock_library_conf.folio_password = ""  # noqa: 105
        mock_migrator = Mock(spec=LoansMigrator)
        mock_migrator.tenant_timezone = ZoneInfo("UTC")
        mock_migrator.migration_report = MigrationReport()
        mock_migrator.service_point_mapping = None
        mock_migrator.failed = {}
        mock_migrator.failed_and_not_dupe = {}
        a = LoansMigrator.load_and_validate_legacy_loans(
            mock_migrator, reader, "Set on file or config"
        )
        assert a[0].service_point_id == "Set in source data"


def test_load_and_validate_legacy_loans_set_centrally():
    with StringIO() as csvfile:
        csvfile.seek(0)
        fieldnames = [
            "item_barcode",
            "patron_barcode",
            "due_date",
            "out_date",
            "renewal_count",
            "next_item_status",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "item_barcode": "i_barcode",
                "patron_barcode": "p_barcode",
                "due_date": "2020-10-12T02:02:02",
                "out_date": "2020-09-12T02:02:02",
                "renewal_count": "1",
                "next_item_status": "",
            }
        )
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)

        mock_library_conf = Mock(spec=LibraryConfiguration)
        mock_library_conf.gateway_url = "https://okapi_url"
        mock_library_conf.tenant_id = ""
        mock_library_conf.folio_username = ""
        mock_library_conf.folio_password = ""  # noqa: 105
        mock_migrator = Mock(spec=LoansMigrator)
        mock_migrator.migration_report = MigrationReport()
        mock_migrator.tenant_timezone = ZoneInfo("UTC")
        mock_migrator.service_point_mapping = None
        mock_migrator.failed = {}
        mock_migrator.failed_and_not_dupe = {}
        a = LoansMigrator.load_and_validate_legacy_loans(
            mock_migrator, reader, "Set on file or config"
        )
        assert a[0].service_point_id == "Set on file or config"


def test_load_and_validate_legacy_loans_with_proxy():
    with StringIO() as csvfile:
        csvfile.seek(0)
        fieldnames = [
            "item_barcode",
            "patron_barcode",
            "proxy_patron_barcode",
            "due_date",
            "out_date",
            "renewal_count",
            "next_item_status",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "item_barcode": "i_barcode",
                "patron_barcode": "p_barcode",
                "proxy_patron_barcode": "prox_barcode",
                "due_date": "2020-10-12T02:02:02",
                "out_date": "2020-09-12T02:02:02",
                "renewal_count": "1",
                "next_item_status": "",
            }
        )
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)

        mock_library_conf = Mock(spec=LibraryConfiguration)
        mock_library_conf.gateway_url = "https://okapi_url"
        mock_library_conf.tenant_id = ""
        mock_library_conf.folio_username = ""
        mock_library_conf.folio_password = ""  # noqa: 105
        mock_migrator = Mock(spec=LoansMigrator)
        mock_migrator.migration_report = MigrationReport()
        mock_migrator.tenant_timezone = ZoneInfo("UTC")
        mock_migrator.service_point_mapping = None
        mock_migrator.failed = {}
        mock_migrator.failed_and_not_dupe = {}
        a = LoansMigrator.load_and_validate_legacy_loans(
            mock_migrator, reader, "Set on file or config"
        )
        assert a[0].proxy_patron_barcode == "prox_barcode"

class DummyLegacyLoan:
    def __init__(
        self, item_barcode="item1", patron_barcode="patron1", proxy_patron_barcode="", row=1
    ):
        self.item_barcode = item_barcode
        self.patron_barcode = patron_barcode
        self.proxy_patron_barcode = proxy_patron_barcode
        self.row = row
        self.to_dict = lambda: {
            "item_barcode": self.item_barcode,
            "patron_barcode": self.patron_barcode,
            "proxy_patron_barcode": self.proxy_patron_barcode,
        }
        self.next_item_status = ""
        self.renewal_count = 0


@pytest.fixture
def migrator():
    m = Mock(spec=LoansMigrator)
    m.failed = {}
    m.migration_report = Mock()
    m.set_renewal_count = Mock()
    m.set_new_status = Mock()
    m.handle_checkout_failure = Mock()
    m.circulation_helper = Mock()
    return m


@patch("folio_migration_tools.migration_tasks.loans_migrator.i18n", autospec=True)
def test_checkout_single_loan_success(mock_i18n, migrator):
    legacy_loan = DummyLegacyLoan()
    res_checkout = Mock()
    res_checkout.was_successful = True
    migrator.circulation_helper.check_out_by_barcode.return_value = res_checkout

    mock_i18n.t.return_value = 'Checked out on first try'

    LoansMigrator.checkout_single_loan(migrator, legacy_loan)

    migrator.migration_report.add.assert_called_with("Details", mock_i18n.t.return_value)
    migrator.set_renewal_count.assert_called_once_with(legacy_loan, res_checkout)
    migrator.set_new_status.assert_called_once_with(legacy_loan, res_checkout)


@patch("folio_migration_tools.migration_tasks.loans_migrator.i18n", autospec=True)
def test_checkout_single_loan_retry_success(mock_i18n, migrator):
    legacy_loan = DummyLegacyLoan()
    res_checkout = Mock()
    res_checkout.was_successful = False
    res_checkout.should_be_retried = True
    res_checkout2 = Mock()
    res_checkout2.was_successful = True
    res_checkout2.folio_loan = True
    migrator.circulation_helper.check_out_by_barcode.return_value = res_checkout
    migrator.handle_checkout_failure.return_value = res_checkout2

    mock_i18n.t.return_value = 'Checked out on second try'

    LoansMigrator.checkout_single_loan(migrator, legacy_loan)

    migrator.migration_report.add.assert_any_call("Details", mock_i18n.t.return_value)
    migrator.set_renewal_count.assert_called_once_with(legacy_loan, res_checkout2)
    migrator.set_new_status.assert_called_once_with(legacy_loan, res_checkout2)


# --- Tests for pre_validate_patron_barcodes_async ---


class TestPreValidatePatronBarcodesAsync:
    def _make_migrator(self, loans, patron_identifiers=None):
        m = Mock(spec=LoansMigrator)
        m.semi_valid_legacy_loans = loans
        m.patron_identifiers = patron_identifiers or ["barcode", "externalSystemId"]
        m.folio_client = Mock()
        m.folio_client.folio_get_async = AsyncMock()
        m.valid_patron_map = {}
        return m

    @pytest.mark.asyncio
    async def test_valid_patron_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P001")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.return_value = [{"barcode": "P001", "id": "uuid-1", "patronGroup": "group-1"}]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_no_patron_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P002")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.return_value = []

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {}

    @pytest.mark.asyncio
    async def test_multiple_patrons_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P003")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.return_value = [
            {"barcode": "P003", "id": "uuid-1"},
            {"barcode": "P003", "id": "uuid-2"},
        ]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {}

    @pytest.mark.asyncio
    async def test_patron_without_barcode_field(self):
        loans = [DummyLegacyLoan(patron_barcode="P004")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.return_value = [{"id": "uuid-1", "username": "someuser"}]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {}

    @pytest.mark.asyncio
    async def test_deduplicates_barcodes(self):
        loans = [
            DummyLegacyLoan(patron_barcode="P001"),
            DummyLegacyLoan(patron_barcode="P001"),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.return_value = [{"barcode": "P001", "id": "uuid-1", "patronGroup": "group-1"}]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        # Should only call the API once for the deduplicated barcode
        assert m.folio_client.folio_get_async.call_count == 1
        assert m.valid_patron_map == {"P001": "P001"}

    @pytest.mark.asyncio
    async def test_includes_proxy_barcodes(self):
        loans = [DummyLegacyLoan(patron_barcode="P001", proxy_patron_barcode="PROXY1")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.side_effect = [
            [{"barcode": "P001", "id": "uuid-1", "patronGroup": "group-1"}],
            [{"barcode": "PROXY1", "id": "uuid-2", "patronGroup": "group-2"}],
        ]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.folio_client.folio_get_async.call_count == 2
        assert "P001" in m.valid_patron_map
        assert "PROXY1" in m.valid_patron_map

    @pytest.mark.asyncio
    async def test_builds_query_with_patron_identifiers(self):
        loans = [DummyLegacyLoan(patron_barcode="P001")]
        m = self._make_migrator(loans, patron_identifiers=["barcode", "externalSystemId"])
        m.folio_client.folio_get_async.return_value = [{"barcode": "P001", "id": "uuid-1"}]

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        m.folio_client.folio_get_async.assert_called_once_with(
            "/users", key="users", query="barcode==P001 OR externalSystemId==P001",
        )

    @pytest.mark.asyncio
    async def test_handles_exception_gracefully(self):
        loans = [DummyLegacyLoan(patron_barcode="P001")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get_async.side_effect = Exception("Connection refused")

        await LoansMigrator.pre_validate_patron_barcodes_async(m)

        assert m.valid_patron_map == {}


# --- Tests for pre_validate_item_barcodes ---


class TestPreValidateItemBarcodes:
    def _make_migrator(self, loans):
        m = Mock(spec=LoansMigrator)
        m.semi_valid_legacy_loans = loans
        m.folio_client = Mock()
        m.valid_item_barcodes = set()
        return m

    def test_all_items_found(self):
        loans = [
            DummyLegacyLoan(item_barcode="I001"),
            DummyLegacyLoan(item_barcode="I002"),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_post.return_value = {
            "items": [
                {"barcode": "I001", "id": "item-uuid-1"},
                {"barcode": "I002", "id": "item-uuid-2"},
            ]
        }

        LoansMigrator.pre_validate_item_barcodes(m)

        assert m.valid_item_barcodes == {"I001", "I002"}

    def test_some_items_missing(self):
        loans = [
            DummyLegacyLoan(item_barcode="I001"),
            DummyLegacyLoan(item_barcode="I002"),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_post.return_value = {
            "items": [{"barcode": "I001", "id": "item-uuid-1"}]
        }

        LoansMigrator.pre_validate_item_barcodes(m)

        assert m.valid_item_barcodes == {"I001"}
        assert "I002" not in m.valid_item_barcodes

    def test_no_items_found(self):
        loans = [DummyLegacyLoan(item_barcode="I001")]
        m = self._make_migrator(loans)
        m.folio_client.folio_post.return_value = {"items": []}

        LoansMigrator.pre_validate_item_barcodes(m)

        assert m.valid_item_barcodes == set()

    def test_deduplicates_item_barcodes(self):
        loans = [
            DummyLegacyLoan(item_barcode="I001"),
            DummyLegacyLoan(item_barcode="I001"),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_post.return_value = {
            "items": [{"barcode": "I001", "id": "item-uuid-1"}]
        }

        LoansMigrator.pre_validate_item_barcodes(m)

        # Single query with deduplicated barcodes
        assert m.folio_client.folio_post.call_count == 1
        assert m.valid_item_barcodes == {"I001"}

    def test_skips_loans_without_item_barcode(self):
        loans = [
            DummyLegacyLoan(item_barcode="I001"),
            DummyLegacyLoan(item_barcode=""),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_post.return_value = {
            "items": [{"barcode": "I001", "id": "item-uuid-1"}]
        }

        LoansMigrator.pre_validate_item_barcodes(m)

        # The query should only contain I001, not empty string
        call_args = m.folio_client.folio_post.call_args
        assert 'barcode=="I001"' in call_args[0][1]["query"]

    def test_batches_requests(self):
        """Verifies that large sets of barcodes are split into batches."""
        loans = [DummyLegacyLoan(item_barcode=f"I{i:04d}") for i in range(5)]
        m = self._make_migrator(loans)
        # Return different items per batch call
        m.folio_client.folio_post.side_effect = [
            {"items": [
                {"barcode": "I0000", "id": "uuid-0"},
                {"barcode": "I0001", "id": "uuid-1"},
            ]},
            {"items": [
                {"barcode": "I0002", "id": "uuid-2"},
                {"barcode": "I0003", "id": "uuid-3"},
            ]},
            {"items": [{"barcode": "I0004", "id": "uuid-4"}]},
        ]

        LoansMigrator.pre_validate_item_barcodes(m, batch_size=2)

        assert m.folio_client.folio_post.call_count == 3
        assert m.valid_item_barcodes == {"I0000", "I0001", "I0002", "I0003", "I0004"}


# --- Tests for check_barcodes ---


class TestCheckBarcodes:
    def _make_migrator(self, loans):
        m = Mock(spec=LoansMigrator)
        m.semi_valid_legacy_loans = loans
        m.failed = {}
        m.migration_report = Mock()
        m.valid_item_barcodes = set()
        m.valid_patron_map = {}
        m.pre_validate_item_barcodes = Mock()
        m.pre_validate_patron_barcodes_async = AsyncMock()
        return m

    @pytest.mark.asyncio
    async def test_yields_loan_when_all_barcodes_valid(self):
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == [loan]

    @pytest.mark.asyncio
    async def test_discards_loan_with_invalid_item_barcode(self):
        loan = DummyLegacyLoan(item_barcode="I999", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == []
        assert "I999" in m.failed

    @pytest.mark.asyncio
    async def test_discards_loan_with_invalid_patron_barcode(self):
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P999")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == []
        assert "I001" in m.failed


def test_get_tenant_timezone_success():
    migrator = Mock(spec=LoansMigrator)
    migrator.folio_client = Mock()
    migrator.folio_client.folio_get_single_object.return_value = {
        "configs": [{"value": json.dumps({"timezone": "America/Chicago"})}]
    }

    LoansMigrator.get_tenant_timezone(migrator, "/configurations/entries?query=timezone")

    assert migrator.tenant_timezone_str == "America/Chicago"


def test_get_tenant_timezone_falls_back_to_utc_when_missing_value():
    migrator = Mock(spec=LoansMigrator)
    migrator.folio_client = Mock()
    migrator.folio_client.folio_get_single_object.return_value = {"configs": [{}]}

    LoansMigrator.get_tenant_timezone(migrator, "/configurations/entries?query=timezone")

    assert migrator.tenant_timezone_str == "UTC"


@pytest.mark.asyncio
async def test_pre_validate_patron_barcodes_async_rejects_patron_without_group():
    migrator = Mock(spec=LoansMigrator)
    migrator.semi_valid_legacy_loans = [DummyLegacyLoan(patron_barcode="P001")]
    migrator.patron_identifiers = ["barcode"]
    migrator.folio_client = Mock()
    migrator.folio_client.folio_get_async = AsyncMock(
        return_value=[{"id": "uuid-1", "barcode": "P001", "patronGroup": ""}]
    )

    await LoansMigrator.pre_validate_patron_barcodes_async(migrator)

    assert migrator.valid_patron_map == {}


@pytest.mark.asyncio
async def test_pre_validate_patron_barcodes_async_rejects_patron_without_barcode():
    migrator = Mock(spec=LoansMigrator)
    migrator.semi_valid_legacy_loans = [DummyLegacyLoan(patron_barcode="P001")]
    migrator.patron_identifiers = ["barcode"]
    migrator.folio_client = Mock()
    migrator.folio_client.folio_get_async = AsyncMock(
        return_value=[{"id": "uuid-1", "patronGroup": "group-1"}]
    )

    await LoansMigrator.pre_validate_patron_barcodes_async(migrator)

    assert migrator.valid_patron_map == {}


def test_pre_validate_item_barcodes_handles_non_dict_response():
    migrator = Mock(spec=LoansMigrator)
    migrator.semi_valid_legacy_loans = [DummyLegacyLoan(item_barcode="I001")]
    migrator.folio_client = Mock()
    migrator.folio_client.folio_post.return_value = []

    LoansMigrator.pre_validate_item_barcodes(migrator)

    assert migrator.valid_item_barcodes == set()


@patch("folio_migration_tools.migration_tasks.loans_migrator.i18n", autospec=True)
def test_declare_lost_uses_fallback_service_point_id_without_cast(mock_i18n):
    migrator = Mock(spec=LoansMigrator)
    migrator.task_configuration = Mock()
    migrator.task_configuration.fallback_service_point_id = "sp-id-123"
    migrator.folio_put_post = Mock(return_value=True)
    migrator.migration_report = Mock()

    folio_loan = {"id": "loan-1", "dueDate": "2024-01-01T00:00:00+00:00"}

    LoansMigrator.declare_lost(migrator, folio_loan)

    _, call_data, _, _ = migrator.folio_put_post.call_args[0]
    assert call_data["servicePointId"] == "sp-id-123"

    @pytest.mark.asyncio
    async def test_discards_loan_with_invalid_proxy_barcode(self):
        loan = DummyLegacyLoan(
            item_barcode="I001", patron_barcode="P001", proxy_patron_barcode="PROXY_BAD"
        )
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == []
        assert "I001" in m.failed

    @pytest.mark.asyncio
    async def test_yields_loan_with_valid_proxy_barcode(self):
        loan = DummyLegacyLoan(
            item_barcode="I001", patron_barcode="P001", proxy_patron_barcode="PROXY1"
        )
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001", "PROXY1": "PROXY1"}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == [loan]

    @pytest.mark.asyncio
    async def test_calls_pre_validation_methods(self):
        m = self._make_migrator([])

        [loan async for loan in LoansMigrator.check_barcodes(m)]

        m.pre_validate_item_barcodes.assert_called_once()
        m.pre_validate_patron_barcodes_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_validation_results_discards_all_loans(self):
        """If pre-validation returns nothing, all loans should be discarded."""
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = set()
        m.valid_patron_map = {}

        result = [loan async for loan in LoansMigrator.check_barcodes(m)]

        assert result == []
        assert "I001" in m.failed


class TestServicePointMappingInit:
    """Test _init_service_point_mapping using real RefDataMapping and mocked FolioClient."""

    def test_creates_ref_data_mapping_from_tsv_file(self, tmp_path):
        """Integration: loads a real TSV mapping file and produces a RefDataMapping."""
        import csv

        csv.register_dialect("tsv", delimiter="\t")
        map_file = tmp_path / "sp_map.tsv"
        map_file.write_text("service_point_id\tfolio_code\nold_desk\tlmd\n*\tfo\n")

        m = Mock(spec=LoansMigrator)
        m.folio_client = mocked_classes.mocked_folio_client()
        m.folder_structure = Mock()
        m.folder_structure.mapping_files_folder = tmp_path
        m.load_ref_data_mapping_file = LoansMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = "sp_map.tsv"

        LoansMigrator._init_service_point_mapping(m, task_config)

        assert isinstance(m.service_point_mapping, RefDataMapping)
        assert m.service_point_mapping.default_id == "finance_office_uuid"
        assert m.service_point_mapping.regular_mappings[0]["folio_id"] == "library_main_desk_uuid"

    def test_skips_loading_when_no_map_file_configured(self):
        m = Mock(spec=LoansMigrator)
        m.load_ref_data_mapping_file = LoansMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = ""

        LoansMigrator._init_service_point_mapping(m, task_config)

        assert m.service_point_mapping is None

    def test_sets_none_when_map_file_does_not_exist(self, tmp_path):
        """When the configured file doesn't exist, service_point_mapping stays None."""
        m = Mock(spec=LoansMigrator)
        m.folio_client = mocked_classes.mocked_folio_client()
        m.folder_structure = Mock()
        m.folder_structure.mapping_files_folder = tmp_path
        m.load_ref_data_mapping_file = LoansMigrator.load_ref_data_mapping_file

        task_config = Mock()
        task_config.service_point_map_file_name = "nonexistent.tsv"

        LoansMigrator._init_service_point_mapping(m, task_config)

        assert m.service_point_mapping is None


@pytest.mark.parametrize(
    ("lost_type", "label"), [("Aged to lost", "Perdu"), ("Declared lost", "Déclaré perdu")]
)
def test_handle_lost_item_translates_label_when_checked_out(report_language, lost_type, label):
    migrator = Mock(spec=LoansMigrator)
    migrator.circulation_helper = Mock()
    migrator.circulation_helper.is_checked_out.return_value = True

    result = LoansMigrator.handle_lost_item(migrator, DummyLegacyLoan(), lost_type)

    assert result.migration_report_message == f"{label} et emprunté"
    assert result.error_message == f"{lost_type} and checked out"


@pytest.mark.parametrize(
    ("lost_type", "label"), [("Aged to lost", "Perdu"), ("Declared lost", "Déclaré perdu")]
)
def test_handle_lost_item_keeps_folio_status_untranslated(report_language, lost_type, label):
    migrator = Mock(spec=LoansMigrator)
    migrator.circulation_helper = Mock()
    migrator.circulation_helper.is_checked_out.return_value = False
    migrator.migration_report = Mock()
    _bind(migrator, "checkout_with_item_temporarily_available")
    legacy_loan = DummyLegacyLoan()

    LoansMigrator.handle_lost_item(migrator, legacy_loan, lost_type)

    assert legacy_loan.next_item_status == lost_type
    (_, measure), _ = migrator.migration_report.add.call_args
    assert f"« {label} »" in measure


def _http_response(status_code, body='{"errors": [{"message": "bad"}]}'):
    import httpx

    return httpx.Response(status_code, text=body, request=httpx.Request("POST", "http://folio/x"))


class TestHttpErrorHandling:
    @pytest.fixture
    def m(self):
        m = Mock(spec=LoansMigrator)
        m.folio_client = Mock()
        m.http_client = Mock()
        m.migration_report = Mock()
        m.failed = {}
        return m

    @pytest.mark.parametrize("status", [200, 201, 204])
    def test_folio_put_post_success(self, m, status):
        m.http_client.post.return_value = _http_response(status, "")
        assert LoansMigrator.folio_put_post(m, "/x", {}, "POST", "act") is True

    @pytest.mark.parametrize("status", [422, 500])
    def test_folio_put_post_http_error(self, m, status):
        m.http_client.put.return_value = _http_response(status)
        assert LoansMigrator.folio_put_post(m, "/x", {}, "PUT", "act") is False

    def test_folio_put_post_connection_error(self, m):
        import httpx

        m.http_client.put.side_effect = httpx.ConnectError("boom")
        assert LoansMigrator.folio_put_post(m, "/x", {}, "PUT", "act") is False

    def test_requests_use_relative_paths_and_client_headers(self, m):
        m.http_client.put.return_value = _http_response(204, "")
        m.http_client.post.return_value = _http_response(201, "")
        m.http_client.get.return_value = _http_response(200, '{"users": [{"id": "u1"}]}')

        LoansMigrator.folio_put_post(m, "/users/u1", {}, "PUT", "act")
        LoansMigrator.folio_put_post(m, "/circulation/loans", {}, "POST", "act")
        LoansMigrator.update_open_loan(m, {"id": "l1", "metadata": {}}, self._loan())
        LoansMigrator.get_user_by_barcode(m, "p1")

        put_calls = m.http_client.put.call_args_list
        assert [c.args[0] for c in put_calls] == ["/users/u1", "/circulation/loans/l1"]
        assert m.http_client.post.call_args.args[0] == "/circulation/loans"
        assert m.http_client.get.call_args.args[0] == "/users"
        assert m.http_client.get.call_args.kwargs["params"] == {"query": '(barcode=="p1")'}
        all_calls = put_calls + [m.http_client.post.call_args, m.http_client.get.call_args]
        assert all("headers" not in c.kwargs for c in all_calls)

    def test_set_item_status_uses_relative_path(self, m):
        m.http_client.get.return_value = _http_response(
            200, '{"items": [{"id": "i1", "status": {"name": "Available"}}]}'
        )
        m.update_item = Mock(return_value=True)
        legacy_loan = DummyLegacyLoan()
        legacy_loan.next_item_status = "Claimed returned"

        assert LoansMigrator.set_item_status(m, legacy_loan) is True

        get_call = m.http_client.get.call_args
        assert get_call.args[0] == "/item-storage/items"
        assert get_call.kwargs["params"] == {"query": '(barcode=="item1")'}
        assert "headers" not in get_call.kwargs

    def _loan(self):
        loan = Mock()
        loan.due_date = "2024-01-02T00:00:00+00:00"
        loan.out_date = "2024-01-01T00:00:00+00:00"
        loan.renewal_count = 0
        return loan

    def test_update_open_loan_http_error(self, m):
        m.http_client.put.return_value = _http_response(500)
        folio_loan = {"id": "l1", "metadata": {}}
        assert LoansMigrator.update_open_loan(m, folio_loan, self._loan()) is False

    def test_update_open_loan_connection_error(self, m):
        import httpx

        m.http_client.put.side_effect = httpx.ConnectError("boom")
        folio_loan = {"id": "l1", "metadata": {}}
        assert LoansMigrator.update_open_loan(m, folio_loan, self._loan()) is False

    def test_declare_lost_failure_is_reported(self, m):
        m.task_configuration = Mock(fallback_service_point_id="sp")
        m.folio_put_post = Mock(return_value=False)
        LoansMigrator.declare_lost(m, {"id": "l1", "dueDate": "2024-01-01T00:00:00+00:00"})
        measures = [c.args[1] for c in m.migration_report.add.call_args_list]
        assert "Unsuccessfully declared loan as lost" in measures

    def test_claim_returned_failure_is_reported(self, m):
        m.folio_put_post = Mock(return_value=False)
        LoansMigrator.claim_returned(m, {"id": "l1", "dueDate": "2024-01-01T00:00:00+00:00"})
        measures = [c.args[1] for c in m.migration_report.add.call_args_list]
        assert any("Unsuccessfully declared loan" in x for x in measures)

    def test_set_item_status_failure_returns_false(self, m):
        legacy = Mock(item_barcode="B1", next_item_status="Lost and paid")
        m.http_client.get.return_value = _http_response(
            200, '{"items": [{"id": "i1", "status": {"name": "Available"}}]}'
        )
        m.update_item = Mock(return_value=False)
        assert LoansMigrator.set_item_status(m, legacy) is False
        # Callers decide whether the loan failed
        assert "B1" not in m.failed

    def test_set_item_status_item_not_found_returns_false(self, m):
        legacy = Mock(item_barcode="B1", next_item_status="Lost and paid")
        m.http_client.get.return_value = _http_response(200, '{"items": []}')
        m.update_item = Mock(return_value=True)
        assert LoansMigrator.set_item_status(m, legacy) is False
        m.update_item.assert_not_called()

    def test_set_item_status_success_returns_true(self, m):
        legacy = Mock(item_barcode="B1", next_item_status="Lost and paid")
        m.http_client.get.return_value = _http_response(
            200, '{"items": [{"id": "i1", "status": {"name": "Available"}}]}'
        )
        m.update_item = Mock(return_value=True)
        assert LoansMigrator.set_item_status(m, legacy) is True
        assert m.update_item.call_args.args[0]["status"]["name"] == "Lost and paid"

    def test_activate_user_does_not_report_success_on_failure(self, m):
        m.update_user = Mock(return_value=False)
        assert LoansMigrator.activate_user(m, {"id": "u1"}) is False
        measures = [c.args[1] for c in m.migration_report.add.call_args_list]
        assert "Successfully activated user" not in measures
        assert "Failed to activate user" in measures

    def test_deactivate_user_does_not_report_success_on_failure(self, m):
        m.update_user = Mock(return_value=False)
        assert LoansMigrator.deactivate_user(m, {"id": "u1"}, "2030-01-01") is False
        measures = [c.args[1] for c in m.migration_report.add.call_args_list]
        assert "Successfully deactivated user" not in measures
        assert "Failed to deactivate user" in measures

    def test_activate_user_reports_success(self, m):
        m.update_user = Mock(return_value=True)
        assert LoansMigrator.activate_user(m, {"id": "u1"}) is True


# --- Follow-up handling after checkout (inactive users, item statuses, renewals) ---


INACTIVE_MESSAGE = "Cannot check out to inactive user"
CLAIMED_RETURNED_MESSAGE = "Item has the item status Claimed returned and cannot be checked out"


def _result(was_successful, error_message="", folio_loan=None, should_be_retried=False):
    from folio_migration_tools.transaction_migration.transaction_result import (
        TransactionResult,
    )

    return TransactionResult(
        was_successful, should_be_retried, folio_loan, error_message, error_message
    )


def _bind(m, *names):
    """Use the real LoansMigrator implementation for these methods on the mock."""
    for name in names:
        setattr(m, name, getattr(LoansMigrator, name).__get__(m))


class TestCheckoutFollowups:
    @pytest.fixture
    def m(self):
        m = Mock(spec=LoansMigrator)
        m.failed = {}
        m.failed_and_not_dupe = {}
        m.followups = []
        m.migration_report = Mock()
        m.circulation_helper = Mock()
        m.update_user = Mock(return_value=True)
        _bind(m, "add_followup")
        return m

    def _inactive_user_setup(self, m, user):
        _bind(m, "checkout_to_inactive_user", "activate_user", "deactivate_user")
        m.get_user_by_barcode = Mock(return_value=user)
        sent = []
        m.update_user.side_effect = lambda u: sent.append(copy.deepcopy(u)) or True
        return sent

    def _stats(self, m):
        return [c.args[0] for c in m.migration_report.add_general_statistics.call_args_list]

    def test_inactive_user_activation_failure_skips_checkout(self, m):
        _bind(m, "checkout_to_inactive_user", "activate_user", "deactivate_user")
        m.get_user_by_barcode = Mock(return_value={"id": "u1", "active": False})
        m.update_user.return_value = False

        res = LoansMigrator.checkout_to_inactive_user(m, DummyLegacyLoan())

        assert not res.was_successful
        assert not res.should_be_retried
        m.circulation_helper.check_out_by_barcode.assert_not_called()
        m.update_user.assert_called_once()

    def test_inactive_user_not_found_skips_checkout(self, m):
        _bind(m, "checkout_to_inactive_user")
        m.get_user_by_barcode = Mock(return_value=None)

        res = LoansMigrator.checkout_to_inactive_user(m, DummyLegacyLoan())

        assert not res.was_successful
        assert not res.should_be_retried
        m.circulation_helper.check_out_by_barcode.assert_not_called()
        m.update_user.assert_not_called()

    def test_user_that_stays_inactive_is_retried_only_once(self, m):
        from folio_migration_tools.custom_exceptions import TransformationRecordFailedError

        _bind(m, "checkout_single_loan", "handle_checkout_failure")
        self._inactive_user_setup(m, {"id": "u1", "active": False})
        m.circulation_helper.check_out_by_barcode.side_effect = lambda loan: _result(
            False, INACTIVE_MESSAGE, should_be_retried=True
        )

        with pytest.raises(TransformationRecordFailedError):
            LoansMigrator.checkout_single_loan(m, DummyLegacyLoan())

        assert m.circulation_helper.check_out_by_barcode.call_count == 2
        assert m.update_user.call_count == 2  # one activate, one deactivate
        assert "item1" in m.failed

    def test_inactive_user_then_item_status_is_handled_while_user_is_active(self, m):
        _bind(
            m,
            "checkout_single_loan",
            "handle_checkout_failure",
            "handle_claimed_returned_item",
            "checkout_with_item_temporarily_available",
        )
        self._inactive_user_setup(m, {"id": "u1", "active": False})
        events = []
        m.update_user.side_effect = lambda u: events.append(("active", u["active"])) or True
        m.set_item_status = Mock(
            side_effect=lambda loan: events.append(("status", loan.next_item_status)) or True
        )
        m.circulation_helper.is_checked_out.return_value = False
        results = [
            _result(False, INACTIVE_MESSAGE, should_be_retried=True),
            _result(False, CLAIMED_RETURNED_MESSAGE, should_be_retried=True),
            _result(True, folio_loan={"id": "loan1"}),
        ]

        def check_out(loan):
            events.append(("checkout",))
            return results.pop(0)

        m.circulation_helper.check_out_by_barcode.side_effect = check_out
        legacy_loan = DummyLegacyLoan()

        LoansMigrator.checkout_single_loan(m, legacy_loan)

        assert events == [
            ("checkout",),
            ("active", True),
            ("checkout",),
            ("status", "Available"),
            ("checkout",),
            ("active", False),
        ]
        assert legacy_loan.next_item_status == "Claimed returned"
        m.set_new_status.assert_called_once()
        assert "Successfully checked out" in self._stats(m)
        assert m.failed == {}

    def test_inactive_user_then_failed_retry_is_not_retried_again(self, m):
        from folio_migration_tools.custom_exceptions import TransformationRecordFailedError

        _bind(m, "checkout_single_loan", "handle_checkout_failure")
        self._inactive_user_setup(m, {"id": "u1", "active": False})
        results = [
            _result(False, INACTIVE_MESSAGE, should_be_retried=True),
            _result(False, "No item with barcode item1", should_be_retried=False),
        ]
        m.circulation_helper.check_out_by_barcode.side_effect = lambda loan: results.pop(0)

        with pytest.raises(TransformationRecordFailedError):
            LoansMigrator.checkout_single_loan(m, DummyLegacyLoan())

        assert m.circulation_helper.check_out_by_barcode.call_count == 2
        assert m.update_user.call_count == 2

    @pytest.mark.parametrize("lost_type", ["Aged to lost", "Declared lost"])
    def test_lost_item_status_message_dispatches_to_lost_handler(self, m, lost_type):
        _bind(m, "handle_checkout_failure")
        m.handle_lost_item = Mock(return_value=_result(True))
        legacy_loan = DummyLegacyLoan()
        message = f"Item has the item status {lost_type} and cannot be checked out"

        LoansMigrator.handle_checkout_failure(
            m, legacy_loan, _result(False, message, should_be_retried=True)
        )

        m.handle_lost_item.assert_called_once_with(legacy_loan, lost_type)

    def test_deactivate_removes_expiration_date_the_user_did_not_have(self, m):
        sent = self._inactive_user_setup(m, {"id": "u1", "active": False})
        m.circulation_helper.check_out_by_barcode.return_value = _result(True)

        LoansMigrator.checkout_to_inactive_user(m, DummyLegacyLoan())

        activated, deactivated = sent
        assert activated["active"] is True and "expirationDate" in activated
        assert deactivated["active"] is False
        assert "expirationDate" not in deactivated

    def test_deactivate_restores_original_expiration_date(self, m):
        sent = self._inactive_user_setup(
            m, {"id": "u1", "active": False, "expirationDate": "2020-01-01T00:00:00"}
        )
        m.circulation_helper.check_out_by_barcode.return_value = _result(True)

        LoansMigrator.checkout_to_inactive_user(m, DummyLegacyLoan())

        activated, deactivated = sent
        assert activated["expirationDate"] != "2020-01-01T00:00:00"
        assert deactivated["expirationDate"] == "2020-01-01T00:00:00"

    @patch("folio_migration_tools.migration_tasks.loans_migrator.Helper")
    def test_failed_deactivation_adds_followup(self, mock_helper, m):
        _bind(m, "checkout_to_inactive_user", "activate_user", "deactivate_user")
        m.get_user_by_barcode = Mock(return_value={"id": "u1", "active": False})
        m.update_user.side_effect = [True, False]
        m.circulation_helper.check_out_by_barcode.return_value = _result(
            True, folio_loan={"id": "loan1"}
        )

        res = LoansMigrator.checkout_to_inactive_user(m, DummyLegacyLoan())

        assert res.was_successful
        assert len(m.followups) == 1
        assert m.followups[0]["followup_type"] == "deactivate_user"
        assert m.followups[0]["loan_id"] == "loan1"
        mock_helper.log_data_issue.assert_called_once()

    def test_get_user_by_barcode_returns_none_when_no_users(self, m):
        m.http_client = Mock()
        m.http_client.get.return_value = _http_response(200, '{"users": []}')

        assert LoansMigrator.get_user_by_barcode(m, "p1") is None

    def test_failed_renewal_count_update_adds_followup(self, m):
        legacy_loan = DummyLegacyLoan()
        legacy_loan.renewal_count = 2
        legacy_loan.due_date = "2024-01-02"
        legacy_loan.out_date = "2024-01-01"
        m.update_open_loan = Mock(return_value=False)

        LoansMigrator.set_renewal_count(m, legacy_loan, _result(True, folio_loan={"id": "l1"}))

        assert "Updated renewal count for loan" not in self._stats(m)
        assert "Failed to update renewal count for loan" in self._stats(m)
        assert [f["followup_type"] for f in m.followups] == ["update_loan"]

    @pytest.mark.parametrize(
        ("status", "method", "followup_type"),
        [
            ("Declared lost", "declare_lost", "declare_lost"),
            ("Claimed returned", "claim_returned", "claim_returned"),
            ("Aged to lost", "set_item_status", "set_item_status"),
        ],
    )
    def test_failed_new_status_adds_followup(self, m, status, method, followup_type):
        legacy_loan = DummyLegacyLoan()
        legacy_loan.next_item_status = status
        setattr(m, method, Mock(return_value=False))

        LoansMigrator.set_new_status(m, legacy_loan, _result(True, folio_loan={"id": "l1"}))

        assert [f["followup_type"] for f in m.followups] == [followup_type]
        assert m.followups[0]["loan_id"] == "l1"

    def test_successful_new_status_adds_no_followup(self, m):
        legacy_loan = DummyLegacyLoan()
        legacy_loan.next_item_status = "Aged to lost"
        m.set_item_status = Mock(return_value=True)

        LoansMigrator.set_new_status(m, legacy_loan, _result(True))

        assert m.followups == []

    def _status_recorder(self, m, results):
        statuses = []

        def set_item_status(loan):
            statuses.append(loan.next_item_status)
            return results.pop(0)

        m.set_item_status = Mock(side_effect=set_item_status)
        _bind(m, "checkout_with_item_temporarily_available")
        return statuses

    def test_aged_to_lost_success_sets_status_once(self, m, report_language):
        statuses = self._status_recorder(m, [True])
        m.circulation_helper.is_checked_out.return_value = False
        m.circulation_helper.check_out_by_barcode.return_value = _result(True)
        legacy_loan = DummyLegacyLoan()

        res = LoansMigrator.handle_lost_item(m, legacy_loan, "Aged to lost")

        assert res.was_successful
        # Only the set to Available; set_new_status puts Aged to lost back
        assert statuses == ["Available"]
        assert legacy_loan.next_item_status == "Aged to lost"
        (_, measure), _ = m.migration_report.add.call_args
        assert "Le statut sera rétabli" in measure

    def test_failed_checkout_restores_item_status(self, m):
        statuses = self._status_recorder(m, [True, True])
        m.circulation_helper.check_out_by_barcode.return_value = _result(False, "nope")

        res = LoansMigrator.checkout_with_item_temporarily_available(
            m, DummyLegacyLoan(), "Aged to lost"
        )

        assert not res.was_successful
        assert not res.should_be_retried
        assert statuses == ["Available", "Aged to lost"]
        assert m.followups == []

    def test_failed_checkout_and_failed_restore_adds_followup(self, m):
        self._status_recorder(m, [True, False])
        m.circulation_helper.check_out_by_barcode.return_value = _result(False, "nope")

        LoansMigrator.checkout_with_item_temporarily_available(
            m, DummyLegacyLoan(), "Claimed returned"
        )

        assert [f["followup_type"] for f in m.followups] == ["restore_item_status"]

    def test_failed_set_to_available_fails_loan_without_checkout(self, m):
        from folio_migration_tools.custom_exceptions import TransformationRecordFailedError

        _bind(m, "checkout_single_loan", "handle_checkout_failure", "handle_checked_out_item")
        self._status_recorder(m, [False])
        m.circulation_helper.is_checked_out.return_value = False
        m.circulation_helper.check_out_by_barcode.return_value = _result(
            False, "Item is already checked out", should_be_retried=True
        )
        legacy_loan = DummyLegacyLoan()

        with pytest.raises(TransformationRecordFailedError):
            LoansMigrator.checkout_single_loan(m, legacy_loan)

        # Only the original checkout attempt
        m.circulation_helper.check_out_by_barcode.assert_called_once()
        assert "Failed loans" in self._stats(m)
        assert m.failed["item1"] is legacy_loan

    def test_write_followups_to_file(self, m, tmp_path):
        m.folder_structure = Mock(results_folder=tmp_path, file_template="_t", time_stamp="_1")
        LoansMigrator.add_followup(m, DummyLegacyLoan(), "update_loan", "detail", {"id": "l1"})

        LoansMigrator.write_followups_to_file(m)

        with open(tmp_path / "loans_needing_followup_t_1.tsv", encoding="utf-8") as f:
            rows = list(csv.DictReader(f, dialect="excel-tab"))
        assert rows == [
            {
                "item_barcode": "item1",
                "patron_barcode": "patron1",
                "loan_id": "l1",
                "followup_type": "update_loan",
                "detail": "detail",
            }
        ]

    def test_no_followup_file_without_followups(self, m, tmp_path):
        m.folder_structure = Mock(results_folder=tmp_path, file_template="_t", time_stamp="_1")

        LoansMigrator.write_followups_to_file(m)

        assert list(tmp_path.iterdir()) == []


class TestResponseParsing:
    @pytest.fixture
    def m(self):
        m = Mock(spec=LoansMigrator)
        m.http_client = Mock()
        m.migration_report = Mock()
        return m

    def test_folio_put_post_422_with_non_json_body(self, m):
        m.http_client.post.return_value = _http_response(422, "<html>oops</html>")
        assert LoansMigrator.folio_put_post(m, "/x", {}, "POST", "act") is False

    def test_update_open_loan_any_2xx_is_success(self, m):
        m.http_client.put.return_value = _http_response(200, "{}")
        legacy = Mock(due_date="2024-01-02T00:00:00+00:00", out_date="2024-01-01T00:00:00+00:00")
        legacy.renewal_count = 1
        assert LoansMigrator.update_open_loan(m, {"id": "l1", "metadata": {}}, legacy) is True
