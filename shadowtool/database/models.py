import sqlalchemy as sqla

from shadowtool.configuration.models import *
from shadowtool.database import db_logger


class DatabaseHook:

    def __init__(self, db_config: "DatabaseConfig", connection_name: str, echo: bool = False):
        self.connection_name = connection_name
        self.db_config = db_config
        self.echo = echo

        # cache declare
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

    def init_schema(self, sqla_metadata) -> None:
        """
        Using a sqla metadata object to initialise database

        :param sqla_metadata:
        :return:
        """
        db_logger.warning("initialise DB schemas ...")
        sqla_metadata.create_all(bind=self.engine)

        db_logger.warning("initialisation complete.")
