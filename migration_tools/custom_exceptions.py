class TransfomationError(Exception):
    pass


class TransformationFieldMappingError(TransfomationError):
    """Raised when the a field mapping fails, but the error is not critical.
    The issue should be logged for the library to act upon it"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Data issue. Consider fixing the record.\t{self.id}\t{self.message}\t{self.data_value}"


class TransformationRecordFailedError(TransfomationError):
    """Raised when the a field mapping fails, Error is critical and means tranformation fails"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Critical data issue. Record needs fixing\t{self.id}\t{self.message}\t{self.data_value}"


class TransformationProcessError(TransfomationError):
    """Raised when the mapping fails due to disconnects in ref data. This error should take the process to a halt"""

    def __init__(
        self,
        id,
        message="Critical Process issue. Transformation failed.",
        data_value="",
    ):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Critical Process issue. Check mapping files and reference data \t{self.id}\t{self.message}\t{self.data_value}"
