import logging
import os


class LoggingMixin:
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def log(self):
        """
        access the logger
        """
        try:
            return self.__class__._log
        except AttributeError:
            self.__class__._log = logging.Logger(
                name=self.__class__.__module__ + "." + self.__class__.__name__
            )
            self.set_level()
            self.set_console()
            return self.__class__._log

    @classmethod
    def set_console(cls):
        # set a format which is simpler for console use
        sh = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s : %(name)s : %(levelname)s : %(message)s"
        )
        sh.setFormatter(formatter)
        cls._log.addHandler(sh)

    @classmethod
    def set_level(cls):
        cls._log.setLevel(os.getenv("HP__LOG_LEVEL", "INFO"))
