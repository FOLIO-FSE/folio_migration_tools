import csv
from io import StringIO
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.loans_migrator import LoansMigrator
import pytest
from unittest.mock import Mock, patch


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
        mock_library_conf.gateway_url = "http://okapi_url"
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
        mock_library_conf.gateway_url = "http://okapi_url"
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
        mock_library_conf.gateway_url = "http://okapi_url"
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
    def __init__(self, item_barcode="item1", patron_barcode="patron1", row=1):
        self.item_barcode = item_barcode
        self.patron_barcode = patron_barcode
        self.row = row
        self.to_dict = lambda: {"item_barcode": self.item_barcode, "patron_barcode": self.patron_barcode}
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

    LoansMigrator.checkout_single_loan(migrator, legacy_loan)

    migrator.migration_report.add.assert_any_call("Details", mock_i18n.t.return_value)
    migrator.set_renewal_count.assert_called_once_with(legacy_loan, res_checkout2)
    migrator.set_new_status.assert_called_once_with(legacy_loan, res_checkout2)
