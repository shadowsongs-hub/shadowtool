from typing import Dict

class DatabaseManager:

    """
        1. automatically converts credentials in TOML config to database connections
        2. manages one or more database connections
        3. dynamic query exectuor
        4. ORM layer

    """

    def __init__(self, config_objects: Dict[DatabaseConfig]):
        pass

    def init_hooks(self):
        """
        initialise a pool of hooks from db configs list
        """
        pass
