import logging


class DatabaseManager:
    """
        handles the connection between database and application
    """

    def __init__(self, echo=False):
        self.user = config_object["database"]["user"]
        self.database = config_object["database"]["database"]
        self.password = config_object["database"]["password"]
        self.port = config_object["database"]["port"]
        self.host = config_object["database"]["host"]
        self.echo = echo

        self._conn = None
        self.engine = None

        self.queries = {}
        self._load_queries_from_folders([SQL_PATH])

