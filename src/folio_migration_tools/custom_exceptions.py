import logging
import i18n


class TransfomationError(Exception):
    pass


class TransformationFieldMappingError(TransfomationError):
    """Raised when the a field mapping fails, but the error is not critical.
    The issue should be logged for the library to act upon it"""

    def __init__(self, index_or_id="", message="", data_value=""):
        self.index_or_id = index_or_id or ""
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
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


class TransformationRecordFailedError(TransfomationError):
    """Raised when the a field mapping fails, Error is critical and means tranformation fails"""

    def __init__(self, index_or_id, message="", data_value=""):
        self.index_or_id = index_or_id
        self.message = message
        self.data_value = data_value
        # logging.log(26, f"RECORD FAILED\t{self.id}\t{self.message}\t{self.data_value}")
        super().__init__(self.message)

    def __str__(self):
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


class TransformationProcessError(TransfomationError):
    """Raised when the transformation fails due to incorrect configuraiton,
    mapping or reference data. This error should take the process to a halt."""

    def __init__(
        self,
        index_or_id,
        message="Critical Process issue. Transformation failed."
        " Check configuration, mapping files and reference data",
        data_value="",
    ):
        self.index_or_id = index_or_id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return (
            f"Critical Process issue. Check configuration, mapping files and reference data"
            f"\t{self.index_or_id}\t{self.message}\t{self.data_value}"
        )
