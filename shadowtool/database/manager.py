import os
from typing import Dict, Any, List

from shadowtool.configuration.models import *
from shadowtool.database import db_logger
from shadowtool.database.models import DatabaseHook
from shadowtool.exceptions import DuplicateQueryNameError


class DatabaseManager:

    """
        1. automatically converts credentials in TOML config to database connections
        2. manages one or more database connections
        3. dynamic query exectuor
        4. ORM layer

    """

    def __init__(
            self,
            db_configs: Dict[str, "DatabaseConfig"],
            loaded_queries_path: str = None,
            echo: bool = False
    ):
        self.db_configs = db_configs
        self.echo = echo  # prints SQL statements
        self.loaded_queries_path = loaded_queries_path  # Loading path
        self.loaded_queries = {}
        self._hooks = {}

        # init
        self.load_queries()

    @property
    def hooks(self):
        """
        initialise a pool of hooks from db configs list
        """
        if not self._hooks:

            self._hooks = {
                name: DatabaseHook(
                    db_config=single_dbc,
                    connection_name=name
                )
                for name, single_dbc in self.db_configs.items()
            }

        db_logger.warning(f"Initialising database hooks, detected {len(self._hooks)}")

        return self._hooks

    def load_queries(self) -> None:
        """
        Load all SQL files from a folder.

        Each file could contain multiple SQL queries. Each query must start with the
        "-- name:" token.

        The query itself should start on the following line

        """

        if not os.path.isabs(self.loaded_queries_path):
            db_logger.warning("Loaded queries path is relative, ensure the path is relative to"
                              "the project root. ")

        for file_name in os.listdir(self.loaded_queries_path):
            if ".sql" not in file_name:
                db_logger.warning(f"{file_name} is not a SQL file. Please check or append the `.sql` extension")

                with open(os.path.join(self.loaded_queries_path, file_name), "r") as fin:
                    raw_string = fin.read()

                    # parse queries
                    self.parse_queries(raw_string)

    def parse_queries(self, file_content: str) -> None:
        """
        split the queries when `-- name: name_of_the_query` appears
        :param file_content: a file containing multiple SELECT queries
        :return: a dict -> key: name_of_query, value: actual_query
        """
        rows = [r.lower() for r in file_content.split("\n")]

        current_key = ""
        for row in rows:
            if ("commit" or "begin") in row:
                continue
            elif "-- name:" in row:
                current_key = row.split("-- name:")[1].strip().lower()
                if current_key in self.loaded_queries:
                    raise Exception(f"{current_key} is already used. Resolve duplicated query name!")
                self.loaded_queries[current_key] = ""
            elif len(row) > 0 and current_key in self.loaded_queries.keys():
                self.loaded_queries[current_key] += row + "\n"

