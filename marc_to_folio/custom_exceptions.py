class TransfomationError(Exception):
    pass


class TransformationDataError(TransfomationError):
    """Raised when the a field mapping fails, but the error is not critical"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Data issue. Consider fixing the record.\t{self.id}\t{self.message}\t{self.data_value}"


class TransformationCriticalDataError(TransfomationError):
    """Raised when the a field mapping fails, Error is critical and means tranformation fails"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Critical data issue. Record needs fixing\t{self.id}\t{self.message}\t{self.data_value}"


class TransformationCodeError(TransfomationError):
    """Raised when the mapping fails for a mandatory field"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Coding issue.\t{self.id}\t{self.message}\t{self.data_value}"


class TransformationProcessError(TransfomationError):
    """Raised when the mapping fails due to disconnects in ref data"""

    def __init__(self, id, message="Transformation failed", data_value=""):
        self.id = id
        self.message = message
        self.data_value = data_value
        super().__init__(self.message)

    def __str__(self):
        return f"Process issue. Check mapping files and reference data \t{self.id}\t{self.message}\t{self.data_value}"
