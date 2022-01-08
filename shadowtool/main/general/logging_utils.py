import logging
import os
from typing import Any
from shadowtool.main.vendors.microsoft_teams import MicroSoftTeamsWebHook
from botocore.exceptions import ParamValidationError
from shadowtool.constants import Color, StatusImage
from .aws import get_secret


# these are randomly chosen images, can be configured
ERROR_THEME_COLOR = "#FF0000"
TASK_ERROR_IMAGE = "https://www.flaticon.com/premium-icon/icons/svg/3099/3099728.svg"
ERROR_IMAGE = "https://as2.ftcdn.net/jpg/01/08/24/41/500_F_108244170_7oNK9Z6OdZ4cQFySG988LDdInqbRD2OA.jpg"


class CustomisedLogger(logging.Logger):
    """
    Executes the original logging function and
    sends message to Teams channel
    """

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        try:
            self.hook = MicroSoftTeamsWebHook(
                get_secret(os.getenv("TEAMS_CHANNEL_HOOK"))
            )
        except ParamValidationError:
            self.error(
                msg="Channel Hook not found in environment variables, "
                "not sending error logs to Teams. To enable it, please "
                "set up `TEAMS_CHANNEL_HOOK` in the environment. "
            )
            self.hook = None

    def critical(self, msg, *args, **kwargs) -> None:
        self.log(msg=msg, level=logging.CRITICAL)
        self.error_handle(
            msg=msg,
            exception_type=kwargs.get("exception_type"),
            traceback=kwargs.get("traceback") or "",
            severity="Critical",
        )

    def exception(self, msg, *args, exc_info=True, **kwargs) -> None:
        self.log(msg=msg, level=logging.ERROR)
        self.error_handle(
            msg=msg,
            exception_type=kwargs.get("exception_type"),
            traceback=kwargs.get("traceback") or "",
            severity="Error",
        )

    def error_handle(
        self, msg: Any, exception_type: Any, traceback: Any, severity: str
    ) -> None:
        if self.hook is None:
            # not going to send to Teams
            return

        severity = severity.lower().capitalize()

        status_image = StatusImage.bug_image.value
        facts = [
            {"name": "Exception Type", "value": exception_type},
            {"name": "Severity", "value": severity},
            {
                "name": "Traceback",
                "value": traceback.replace("\n", "<br>"),
            },  # this is to support newline
        ]
        card_json = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": Color.error.value,
            "summary": msg,
            "sections": [
                {
                    "activityTitle": msg,
                    "activityImage": status_image,
                    "facts": facts,
                    "markdown": False,
                }
            ],
        }
        self.hook.send_custom_card(card_json=card_json)


class LoggingMixin:
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def log(self):
        """
            create a logger mixin and it works perfectly
        """
        try:
            return self.__class__._log
        except AttributeError:
            self.__class__._log = CustomisedLogger(
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
