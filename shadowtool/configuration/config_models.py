import attr


@attr.s(auto_attribs=True)
class BaseConfig:
    pass


@attr.s(auto_attribs=True)
class DatabaseConfig(BaseConfig):
    server: str = attr.ib()
    port: int = attr.ib()
    user: str = attr.ib()
    password: str = attr.ib()
    database: str = attr.ib()

    @property
    def sqlalchemy_uri(self):
        return f"postgres://{self.user}:{self.password}@{self.server}:{self.port}/{self.database}"
