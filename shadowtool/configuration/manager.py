from shadowtool.exceptions import InvalidConfigFormatError
from shadowtool.configuration.config_models import *
from shadowtool import global_logger
import toml


class ConfigManager:

    """
        automatically construct config object for various purposes

        requires the config file to have a standardized structure
    """

    def __init__(self, config_path: str):

        with open(config_path, 'r') as file:
            self.parsed_object = toml.loads(file.read())

        self.db_config = self._construct_db_config()

    def _construct_db_config(self):
        if 'database' not in self.parsed_object:
            raise InvalidConfigFormatError

        database_object = self.parsed_object['database']

        db_config = {}

        for credential_name, credential_info in database_object.items():
            DatabaseConfig(**credential_info)
            db_config[credential_name] = DatabaseConfig(**credential_info)

        global_logger.warning(f"{len(db_config)} db credentials parsed. ")

        return db_config


if __name__ == '__main__':
    cm = ConfigManager("../../test-config.toml")
