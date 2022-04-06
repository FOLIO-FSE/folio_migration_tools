from migration_tools.transaction_migration.legacy_loan import LegacyLoan


def test_init():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113",
        "out_date": "20220113",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    legacy_loan = LegacyLoan(loan_dict, 0)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-01-13T23:59:00"
    assert legacy_loan.out_date.isoformat() == "2022-01-13T00:01:00"
    assert legacy_loan.renewal_count > 0


def test_init_tz():
    loan_dict = {
        "item_barcode": "the barcode with trailing space ",
        "patron_barcode": " the barcode with leading space",
        "due_date": "20220113 22:00",
        "out_date": "20220113 20:00",
        "renewal_count": "1",
        "next_item_status": "Checked out",
    }
    legacy_loan = LegacyLoan(loan_dict, -6)
    assert legacy_loan.patron_barcode == "the barcode with leading space"
    assert legacy_loan.item_barcode == "the barcode with trailing space"
    assert legacy_loan.due_date.isoformat() == "2022-01-13T16:00:00"
    assert legacy_loan.out_date.isoformat() == "2022-01-13T14:00:00"
    assert legacy_loan.renewal_count > 0
