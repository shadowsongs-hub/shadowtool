class InvalidLoggingLevelError(Exception):
    pass


class InvalidConfigFormatError(Exception):
    pass


class UnknownLoadedQueryNameError(Exception):
    pass


class EmptyQueryError(Exception):
    pass


class DuplicateQueryNameError(Exception):
    pass


class RetryDBConnectionFailure(Exception):
    pass


class RequestToObjectException(Exception):
    pass


class ConnectionIDNotFound(Exception):
    pass


class BashCommandFailure(Exception):
    pass


class UnknownRegistraModelTypeError(Exception):

    """
    raise when the a new value is past to a enum type model in registra configuration
    """

    def __init__(self, model_name: str, raw_type: str):
        self.message = f"Unknown {model_name} type `{raw_type}`. Please check or declare a new one."
        super().__init__(self.message)
