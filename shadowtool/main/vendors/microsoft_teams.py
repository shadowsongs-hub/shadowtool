"""
This class allows notifications from airflow to send to Teams
"""
import requests
import logging


class MicroSoftTeamsWebHook:
    """
    this class handles the interaction between client and MS Teams channel
    """

    def __init__(self, webhook_link):
        self.webhook_link = webhook_link

    def send_msg(
        self,
        title: str,
        content: str,
        theme_color="00FF00",
        enable_markdown: bool = False,
    ):
        """
        send a simple message with color
        """
        card_json = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": f"{theme_color}",
            "summary": f"{title}",
            "title": f"{title}",
            "sections": [{"text": f"{content}", "markdown": enable_markdown}],
        }
        self._deliver_card(card_json)

    def send_custom_card(self, card_json: dict):
        self._deliver_card(card_json)

    def _deliver_card(self, card_json):
        resp = requests.post(self.webhook_link, json=card_json)
        if resp.status_code == 200:

            logging.info(f"Message successfully delivered.")
        else:
            logging.warning(f"Status code: {resp.status_code}, {resp.content}")
