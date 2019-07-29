import logging


class LoggingManager:

    """

        A simple Logger class. Most of the methods here now are trivial.

    """

    def __init__(self, logging_level: str = 'INFO'):
        self.logging_level = logging_level
        self._logger = None

        # initialisation
        self.logger.setLevel(self.logging_level)
        self._add_console_handler()

        self.logger.warning("Logger initialised.")

    def _validate_logging_level(self):
        return self.logging_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

    @staticmethod
    def get_standard_formatter():
        return logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

        return self._logger

    def _add_console_handler(self):
        console = logging.StreamHandler()
        console.setLevel(self.logging_level)
        console.setFormatter(self.get_standard_formatter())

        self.logger.addHandler(console)


