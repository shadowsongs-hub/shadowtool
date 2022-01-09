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
