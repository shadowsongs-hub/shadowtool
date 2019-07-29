import logging

from shadowtool.exceptions import InvalidLoggingLevel


class LoggingManager:

    """

        A simple Logger class. Most of the methods here now are trivial.

    """

    def __init__(self, global_logging_level: str = 'INFO'):
        self.global_logging_level = global_logging_level

        if not self._validate_logging_level(self.global_logging_level):
            raise InvalidLoggingLevel(f"Logging level is invalid. Input is: {self.global_logging_level}")

    @staticmethod
    def _validate_logging_level(logging_level: str) -> bool:
        return logging_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

    @staticmethod
    def get_standard_formatter() -> logging.Formatter:
        return logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def get_logger(self, logging_level: str = None, logger_name: str = __name__, console: bool=True) -> logging.Logger:
        if logging_level is None:
            logging_level = self.global_logging_level

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging_level)

        if console:
            self._add_console_handler(logger)

        logger.warning(f"Logger **{logger_name}** initialised.")

        return logger

    def _add_console_handler(self, logger: logging.Logger) -> None:
        console = logging.StreamHandler()
        console.setFormatter(self.get_standard_formatter())
        logger.addHandler(console)

