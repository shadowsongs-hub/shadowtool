from typing import Dict

from shadowtool.configuration.models import *
from shadowtool.database import db_logger
from shadowtool.database.models import DatabaseHook


class DatabaseManager:

    """
        1. automatically converts credentials in TOML config to database connections
        2. manages one or more database connections
        3. dynamic query exectuor
        4. ORM layer

    """

    def __init__(self, db_configs: Dict[str, "DatabaseConfig"], echo: bool=False):
        self.db_configs = db_configs
        self.echo = echo  # prints statements

        self._hooks = {}

    @property
    def hooks(self):
        """
        initialise a pool of hooks from db configs list
        """
        if not self._hooks:

            self._hooks = {name: DatabaseHook(
                db_config=single_dbc,
                connection_name=name
            ) for name, single_dbc in self.db_configs.items()}

        db_logger.warning(f"Initialising database hooks, detected {len(self._hooks)}")

        return self._hooks



