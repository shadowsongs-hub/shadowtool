from typing import Dict

import sqlalchemy as sqla

from shadowtool.configuration.config_models import *


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

        self._engines = {}

    @property
    def engines(self):
        """
        initialise a pool of hooks from db configs list
        """
        if not self._engines:
            for connection_name, single_dbc in self.db_configs.items():
                self._engines[connection_name] = sqla.create_engine(
                    single_dbc.sqlalchemy_uri,
                    pool_size=20,
                    echo=self.echo
                )

        return self._engines
