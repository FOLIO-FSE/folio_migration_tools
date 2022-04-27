from typing import Any


class TransactionResult(object):
    __slots__ = [
        "was_successful",
        "folio_loan",
        "should_be_retried",
        "error_message",
        "migration_report_message",
    ]

    def __init__(
        self,
        was_successful: bool,
        should_be_retried: bool,
        folio_loan: Any,
        error_message: str,
        migration_report_message: str,
    ):
        self.was_successful = was_successful
        self.folio_loan = folio_loan
        self.should_be_retried = should_be_retried
        self.error_message = error_message
        self.migration_report_message = migration_report_message
