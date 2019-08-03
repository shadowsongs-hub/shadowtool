from typing import Dict, Any, List, Optional

import sqlalchemy as sqla

from shadowtool.configuration.models import *
from shadowtool.database import db_logger
from shadowtool.exceptions import UnknownLoadedQueryNameError, EmptyQueryError


class DatabaseHook:

    def __init__(
            self,
            db_config: "DatabaseConfig",
            connection_name: str,
            echo: bool = False
    ):
        self.connection_name = connection_name
        self.db_config = db_config
        self.echo = echo

        # cache declare
        self.loaded_queries = None
        self._engine = None

    @property
    def engine(self):
        if not self._engine:
            self._engine = sqla.create_engine(
                self.db_config.sqlalchemy_uri,
                echo=self.echo
            )

        return self._engine

    @property
    def conn(self):
        db_logger.warning(f"Establishing connection to database, using connection: {self.connection_name}")

        connection = self.engine.connect()

        db_logger.warning('connection established.')
        return connection

    def add_loaded_queries(self, loaded_queries: Dict[str, str]):
        self.loaded_queries = loaded_queries

    def init_schema(self, sqla_metadata) -> None:
        """
        Using a sqla metadata object to initialise database

        :param sqla_metadata:
        :return:
        """
        db_logger.warning("initialise DB schemas ...")

        sqla_metadata.create_all(bind=self.engine)

        db_logger.warning("initialisation complete.")

    def execute_query(
            self,
            query: str = None,
            query_name: str = None,
            parameters: Dict[str, Any] = None,
    ):
        """
        Execute a query or a loaded query, parameters are acceptable
        in dictionary format.

        The output would contain a list of dictionary, representing
        the rows.

        Note the loaded query would override the raw query strings
        passed to the method.

        :param query: str, raw query
        :param query_name: str, loaded query name
        :param parameters: dict, SQL parameters
        :return: a list of rows in dictionary format
        """
        if query_name is not None:
            try:
                query = self.loaded_queries[query_name]
            except KeyError:
                raise UnknownLoadedQueryNameError(
                    "Unknown SQL query name. Please check the query is actually in the specified path."
                )

        if query is None:
            raise EmptyQueryError("You need to pass at least the query string.")

        if parameters is None:
            parameters = {}

        with self.conn.begin():
            res = self.conn.execute(sqla.sql.text(query), **parameters)

        if res.returns_rows:
            data = res.fetchall()

            column_names = res.keys()

            final = []
            for row in data:
                final.append(dict(zip(column_names, row)))

            return final


class CRUDBase:

    """

    This class abstracts the common CRUD operations of objects away from the SQLalchemy layer

    """

    def __init__(self):
        pass

    def create(self):
        pass

    def read(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass
