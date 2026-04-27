import csv
from io import StringIO
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.loans_migrator import LoansMigrator
import pytest
from unittest.mock import patch


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


# --- Tests for pre_validate_patron_barcodes ---


class TestPreValidatePatronBarcodes:
    def _make_migrator(self, loans, patron_identifiers=None):
        m = Mock(spec=LoansMigrator)
        m.semi_valid_legacy_loans = loans
        m.patron_identifiers = patron_identifiers or ["barcode", "externalSystemId"]
        m.folio_client = Mock()
        m.valid_patron_map = {}
        return m

    def test_valid_patron_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P001")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.return_value = [{"barcode": "P001", "id": "uuid-1"}]

        LoansMigrator.pre_validate_patron_barcodes(m)

        assert m.valid_patron_map == {"P001": "P001"}

    def test_no_patron_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P002")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.return_value = []

        LoansMigrator.pre_validate_patron_barcodes(m)

        assert m.valid_patron_map == {}

    def test_multiple_patrons_found(self):
        loans = [DummyLegacyLoan(patron_barcode="P003")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.return_value = [
            {"barcode": "P003", "id": "uuid-1"},
            {"barcode": "P003", "id": "uuid-2"},
        ]

        LoansMigrator.pre_validate_patron_barcodes(m)

        assert m.valid_patron_map == {}

    def test_patron_without_barcode_field(self):
        loans = [DummyLegacyLoan(patron_barcode="P004")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.return_value = [{"id": "uuid-1", "username": "someuser"}]

        LoansMigrator.pre_validate_patron_barcodes(m)

        assert m.valid_patron_map == {}

    def test_deduplicates_barcodes(self):
        loans = [
            DummyLegacyLoan(patron_barcode="P001"),
            DummyLegacyLoan(patron_barcode="P001"),
        ]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.return_value = [{"barcode": "P001", "id": "uuid-1"}]

        LoansMigrator.pre_validate_patron_barcodes(m)

        # Should only call the API once for the deduplicated barcode
        assert m.folio_client.folio_get.call_count == 1
        assert m.valid_patron_map == {"P001": "P001"}

    def test_includes_proxy_barcodes(self):
        loans = [DummyLegacyLoan(patron_barcode="P001", proxy_patron_barcode="PROXY1")]
        m = self._make_migrator(loans)
        m.folio_client.folio_get.side_effect = [
            [{"barcode": "P001", "id": "uuid-1"}],
            [{"barcode": "PROXY1", "id": "uuid-2"}],
        ]

        LoansMigrator.pre_validate_patron_barcodes(m)

        assert m.folio_client.folio_get.call_count == 2
        assert "P001" in m.valid_patron_map
        assert "PROXY1" in m.valid_patron_map

    def test_builds_query_with_patron_identifiers(self):
        loans = [DummyLegacyLoan(patron_barcode="P001")]
        m = self._make_migrator(loans, patron_identifiers=["barcode", "externalSystemId"])
        m.folio_client.folio_get.return_value = [{"barcode": "P001", "id": "uuid-1"}]

        LoansMigrator.pre_validate_patron_barcodes(m)

        m.folio_client.folio_get.assert_called_once_with(
            "/users",
            "users",
            query="barcode==P001 OR externalSystemId==P001",
        )


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
        m.pre_validate_patron_barcodes = Mock()
        return m

    def test_yields_loan_when_all_barcodes_valid(self):
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == [loan]

    def test_discards_loan_with_invalid_item_barcode(self):
        loan = DummyLegacyLoan(item_barcode="I999", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == []
        assert "I999" in m.failed

    def test_discards_loan_with_invalid_patron_barcode(self):
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P999")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == []
        assert "I001" in m.failed

    def test_discards_loan_with_invalid_proxy_barcode(self):
        loan = DummyLegacyLoan(
            item_barcode="I001", patron_barcode="P001", proxy_patron_barcode="PROXY_BAD"
        )
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001"}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == []
        assert "I001" in m.failed

    def test_yields_loan_with_valid_proxy_barcode(self):
        loan = DummyLegacyLoan(
            item_barcode="I001", patron_barcode="P001", proxy_patron_barcode="PROXY1"
        )
        m = self._make_migrator([loan])
        m.valid_item_barcodes = {"I001"}
        m.valid_patron_map = {"P001": "P001", "PROXY1": "PROXY1"}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == [loan]

    def test_calls_pre_validation_methods(self):
        m = self._make_migrator([])

        list(LoansMigrator.check_barcodes(m))

        m.pre_validate_item_barcodes.assert_called_once()
        m.pre_validate_patron_barcodes.assert_called_once()

    def test_empty_validation_results_discards_all_loans(self):
        """If pre-validation returns nothing, all loans should be discarded."""
        loan = DummyLegacyLoan(item_barcode="I001", patron_barcode="P001")
        m = self._make_migrator([loan])
        m.valid_item_barcodes = set()
        m.valid_patron_map = {}

        result = list(LoansMigrator.check_barcodes(m))

        assert result == []
        assert "I001" in m.failed
