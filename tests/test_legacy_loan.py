import json
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.transaction_migration.legacy_loan import LegacyLoan
from folio_migration_tools.custom_exceptions import TransformationProcessError, TransformationRecordFailedError
from folio_migration_tools.transaction_migration import legacy_loan as legacy_loan_module


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


def test_init_tz(caplog):
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
        "Provided due_date is not UTC, setting tz-info to tenant timezone (America/Chicago)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided out_date is not UTC, setting tz-info to tenant timezone (America/Chicago)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided due_date is not UTC in row=0, setting tz-info to tenant timezone (America/Chicago)"
        in caplog.text
    )
    assert (
        "Provided out_date is not UTC in row=0, setting tz-info to tenant timezone (America/Chicago)"
        in caplog.text
    )


def test_init_tz_2(caplog):
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
        "Hour and minute not specified for due date"
        in migration_report.report["Details"]
    )

    assert (
        "Provided out_date is not UTC, setting tz-info to tenant timezone (UTC)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided due_date is not UTC, setting tz-info to tenant timezone (UTC)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided out_date is not UTC in row=0, setting tz-info to tenant timezone (UTC)"
        in caplog.text
    )
    assert (
        "Provided due_date is not UTC in row=0, setting tz-info to tenant timezone (UTC)"
        in caplog.text
    )


def test_init_tz_3(caplog):
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
        "Provided due_date is not UTC, setting tz-info to tenant timezone (Australia/Sydney)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided out_date is not UTC, setting tz-info to tenant timezone (Australia/Sydney)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided due_date is not UTC in row=0, setting tz-info to tenant timezone (Australia/Sydney)"
        in caplog.text
    )
    assert (
        "Provided out_date is not UTC in row=0, setting tz-info to tenant timezone (Australia/Sydney)"
        in caplog.text
    )


def test_init_tz_4(caplog):  # Test dates with(out) DST
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
        "Provided due_date is not UTC, setting tz-info to tenant timezone (Australia/Sydney)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided out_date is not UTC, setting tz-info to tenant timezone (Australia/Sydney)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided due_date is not UTC in row=0, setting tz-info to tenant timezone (Australia/Sydney)"
        in caplog.text
    )
    assert (
        "Provided out_date is not UTC in row=0, setting tz-info to tenant timezone (Australia/Sydney)"
        in caplog.text
    )


def test_init_tz_5(caplog):  # Test dates with(out) DST
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
        "Provided due_date is not UTC, setting tz-info to tenant timezone (America/Chicago)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided out_date is not UTC, setting tz-info to tenant timezone (America/Chicago)"
        in migration_report.report["Details"]
    )
    assert (
        "Provided due_date is not UTC in row=0, setting tz-info to tenant timezone (America/Chicago)"
        in caplog.text
    )
    assert (
        "Provided out_date is not UTC in row=0, setting tz-info to tenant timezone (America/Chicago)"
        in caplog.text
    )


def test_init_renewal_count_is_missing():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 16:00",
        "out_date": "20220113 14:00",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(
        loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.renewal_count == 0


def test_init_renewal_count_is_empty():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 16:00",
        "out_date": "20220113 14:00",
        "renewal_count": "",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(
        loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.renewal_count == 0


def test_init_renewal_count_is_unresolvable():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 16:00",
        "out_date": "20220113 14:00",
        "renewal_count": "abc",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(
        loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.renewal_count == 0


def test_to_dict() -> None:
    loan_dict = {
        "item_barcode": "the barcode with trailing spaces    ",
        "patron_barcode": "   the barcode with leading spaces",
        "due_date": "20250327",
        "out_date": "20250327",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    expected_result = {
        "item_barcode": "the barcode with trailing spaces",
        "patron_barcode": "the barcode with leading spaces",
        "proxy_patron_barcode": "",
        "due_date": "2025-03-27T23:59:00+00:00",
        "out_date": "2025-03-27T00:01:00+00:00",
        "renewal_count": 1,
        "next_item_status": "Checked out",
        "service_point_id": ""
    }
    assert legacy_loan.to_dict() == expected_result


def test_correct_for_1_day_loans_time_alignment_is_ok() -> None:
    loan_dict = {
        "item_barcode": "the barcode with trailing spaces    ",
        "patron_barcode": "   the barcode with leading spaces",
        "due_date": "20250327",
        "out_date": "20250327",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    assert legacy_loan.due_date.isoformat() == "2025-03-27T23:59:00+00:00"
    assert legacy_loan.out_date.isoformat() == "2025-03-27T00:01:00+00:00"


def test_correct_for_1_day_loans_due_date_is_before_out_date() -> None:
    loan_dict = {
        "item_barcode": "the barcode with trailing spaces    ",
        "patron_barcode": "   the barcode with leading spaces",
        "due_date": "20250326",
        "out_date": "20250327",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    expected_err_message = (
        "Critical data issue. Record needs fixing\t"
        f"0\tDue date is before out date, or date information is missing from both\t{json.dumps(loan_dict, indent=2)}"
    )
    with pytest.raises(TransformationRecordFailedError, match=expected_err_message):
        LegacyLoan(loan_dict, "", migration_report, tenant_timezone)


def test_correct_for_1_day_loans_no_out_or_due_date_info() -> None:
    loan_dict = {
        "item_barcode": "the barcode with trailing spaces    ",
        "patron_barcode": "   the barcode with leading spaces",
        "due_date": "",
        "out_date": "",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()
    legacy_loan = LegacyLoan(loan_dict, "", migration_report, tenant_timezone)
    # When both dates are missing, LegacyLoan previously defaulted to current UTC dates and
    # adjusted them to keep due_date after out_date without raising.
    assert legacy_loan.due_date > legacy_loan.out_date


def test_parse_errors_use_end_of_day(monkeypatch):
    fake_now = datetime(2024, 1, 2, 12, 30, tzinfo=ZoneInfo("UTC"))

    class DummyDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # pragma: no cover - delegated call
            return fake_now

    monkeypatch.setattr(legacy_loan_module, "datetime", DummyDateTime)

    loan_dict = {
        "item_barcode": "item",
        "patron_barcode": "patron",
        "due_date": "not-a-date",
        "out_date": "not-a-date",
        "renewal_count": "2",
        "next_item_status": "Checked out",
    }
    tenant_timezone = ZoneInfo("UTC")
    migration_report = MigrationReport()

    legacy_loan = LegacyLoan(loan_dict, "fallback-sp", migration_report, tenant_timezone)

    assert legacy_loan.due_date == fake_now.replace(hour=23, minute=59, second=0, microsecond=0)
    assert legacy_loan.out_date == fake_now.replace(hour=0, minute=1, second=0, microsecond=0)
