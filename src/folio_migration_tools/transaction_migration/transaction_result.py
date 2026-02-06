"""Transaction result container for migration operations.

Defines the TransactionResult class for encapsulating the outcome of transaction
migration attempts. Tracks success/failure status, error messages, and whether
retries should be attempted.
"""

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
        """Initialize TransactionResult for migration tracking.

        Args:
            was_successful (bool): Whether the transaction was successfully created.
            should_be_retried (bool): Whether a failed transaction should be retried.
            folio_loan (Any): The created FOLIO transaction object.
            error_message (str): Error message if transaction failed.
            migration_report_message (str): Message for migration report.
        """
        self.was_successful = was_successful
        self.folio_loan = folio_loan
        self.should_be_retried = should_be_retried
        self.error_message = error_message
        self.migration_report_message = migration_report_message
