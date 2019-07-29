from configobj import ConfigObj


class ConfigManager:

    def __init__(self, config_path: str):
        self.config_object = ConfigObj(config_path)
