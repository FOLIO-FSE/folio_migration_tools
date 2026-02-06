"""Custom exception classes for migration transformations.

Defines specialized exception types for different transformation error scenarios:
- TransformationFieldMappingError: Non-critical data issues
- TransformationRecordFailedError: Critical record-level failures
- TransformationProcessError: Fatal configuration or process errors
"""

import logging
import i18n

from folio_migration_tools import StrCoercible


class TransformationError(Exception):
    pass


class TransformationFieldMappingError(TransformationError):
    """Raised when the field mapping fails, but the error is not critical.

    The issue should be logged for the library to act upon it
    """

    def __init__(self, index_or_id="", message="", data_value: str | StrCoercible = ""):
        """Initialize field mapping error with index, message, and data value.

        Args:
            index_or_id: Record identifier or row index from source data.
            message: Descriptive error message.
            data_value: The problematic data value.
        """
        self.index_or_id = index_or_id or ""
        self.message = message
        self.data_value: str | StrCoercible = data_value
        super().__init__(self.message)

    def __str__(self):
        """Return formatted error message with record context."""
        return (
            i18n.t("Data issue. Consider fixing the record. ")
            + f"\t{self.index_or_id}\t{self.message}\t{self.data_value}"
        )

    def log_it(self):
        logging.log(
            26,
            "FIELD MAPPING FAILED\t%s\t%s\t%s",
            self.index_or_id,
            self.message,
            self.data_value,
        )


class TransformationRecordFailedError(TransformationError):
    """Raised when the field mapping fails, Error is critical and means transformation fails."""

    def __init__(self, index_or_id, message="", data_value=""):
        """Initialize record failure error with context information.

        Args:
            index_or_id: Record identifier or row index from source data.
            message: Descriptive error message about the failure.
            data_value: The problematic data value.
        """
        self.index_or_id = index_or_id
        self.message = message
        self.data_value: str | StrCoercible = data_value
        # logging.log(26, f"RECORD FAILED\t{self.id}\t{self.message}\t{self.data_value}")
        super().__init__(self.message)

    def __str__(self):
        """Return formatted error message with record context."""
        return (
            f"Critical data issue. Record needs fixing"
            f"\t{self.index_or_id}\t{self.message}\t{self.data_value}"
        )

    def log_it(self):
        logging.log(
            26,
            "RECORD FAILED\t%s\t%s\t%s",
            self.index_or_id,
            self.message,
            self.data_value,
        )


class TransformationProcessError(TransformationError):
    """Raised when the transformation fails due to incorrect configuration.

    This error should take the process to a halt when mapping or reference data is incorrect.
    """

    def __init__(
        self,
        index_or_id,
        message="Critical Process issue. Transformation failed."
        " Check configuration, mapping files and reference data",
        data_value: str | StrCoercible = "",
    ):
        """Initialize process error with configuration or reference data context.

        Args:
            index_or_id: Record identifier or context for where error occurred.
            message: Descriptive error message about the process failure.
            data_value: The problematic data or configuration value.
        """
        self.index_or_id = index_or_id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        """Return formatted error message with process context."""
        return (
            f"Critical Process issue. Check configuration, mapping files and reference data"
            f"\t{self.index_or_id}\t{self.message}\t{self.data_value}"
        )
