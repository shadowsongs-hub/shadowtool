import attr


@attr.s(auto_attribs=True)
class BaseConfig:
    pass


@attr.s(auto_attribs=True)
class DatabaseConfig:
    server: str = attr.ib()
    port: int = attr.ib()
    user: str = attr.ib()
    password: str = attr.ib()
    database: str = attr.ib()
