from zoneinfo import ZoneInfo

from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.transaction_migration.legacy_loan import LegacyLoan


def test_init():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113",
        "out_date": "20220113",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-01-13T23:59:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2022-01-13T00:01:00+00:00"
    assert legacy_loan.renewal_count > 0
    migration_report.report = {}


def test_init_tz():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 16:00",
        "out_date": "20220113 14:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("America/Chicago")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-01-13T22:00:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2022-01-13T20:00:00+00:00"
    assert legacy_loan.renewal_count > 0
    assert (
        "Provided due_date is not UTC, setting tzinfo to tenant timezone (America/Chicago)"
        in migration_report.report[Blurbs.Details[0]]
    )

    assert (
        "Provided out_date is not UTC, setting tzinfo to tenant timezone (America/Chicago)"
        in migration_report.report[Blurbs.Details[0]]
    )


def test_init_tz_2():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "2019-02-22",
        "out_date": "2019-02-22 10:53:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.due_date.isoformat() == "2019-02-22T23:59:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2019-02-22T10:53:00+00:00"
    assert legacy_loan.renewal_count > 0
    assert (
        "Hour and minute not specified for due date. Assuming end of local calendar day (23:59)..."
        in migration_report.report[Blurbs.Details[0]]
    )

    assert (
        "Provided out_date is not UTC, setting tzinfo to tenant timezone (UTC)"
        in migration_report.report[Blurbs.Details[0]]
    )
    assert (
        "Provided due_date is not UTC, setting tzinfo to tenant timezone (UTC)"
        in migration_report.report[Blurbs.Details[0]]
    )


def test_init_tz_3():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 16:00",
        "out_date": "20220113 14:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("Australia/Sydney")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-01-13T05:00:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2022-01-13T03:00:00+00:00"
    assert legacy_loan.renewal_count > 0
    assert (
        "Provided due_date is not UTC, setting tzinfo to tenant timezone (Australia/Sydney)"
        in migration_report.report[Blurbs.Details[0]]
    )
    assert (
        "Provided out_date is not UTC, setting tzinfo to tenant timezone (Australia/Sydney)"
        in migration_report.report[Blurbs.Details[0]]
    )


def test_init_tz_4():  # Test dates with(out) DST
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220613 16:00",
        "out_date": "20220613 14:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("Australia/Sydney")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-06-13T06:00:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2022-06-13T04:00:00+00:00"
    assert legacy_loan.renewal_count > 0
    assert (
        "Provided due_date is not UTC, setting tzinfo to tenant timezone (Australia/Sydney)"
        in migration_report.report[Blurbs.Details[0]]
    )
    assert (
        "Provided out_date is not UTC, setting tzinfo to tenant timezone (Australia/Sydney)"
        in migration_report.report[Blurbs.Details[0]]
    )


def test_init_tz_5():  # Test dates with(out) DST
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220613 16:00",
        "out_date": "20220613 14:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("America/Chicago")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-06-13T21:00:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2022-06-13T19:00:00+00:00"
    assert legacy_loan.renewal_count > 0
    assert (
        "Provided due_date is not UTC, setting tzinfo to tenant timezone (America/Chicago)"
        in migration_report.report[Blurbs.Details[0]]
    )
    assert (
        "Provided out_date is not UTC, setting tzinfo to tenant timezone (America/Chicago)"
        in migration_report.report[Blurbs.Details[0]]
    )
