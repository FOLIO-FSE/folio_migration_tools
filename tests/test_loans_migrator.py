import csv
from io import StringIO
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.loans_migrator import LoansMigrator


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
