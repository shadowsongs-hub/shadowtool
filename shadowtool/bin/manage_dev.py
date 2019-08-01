import click

from shadowtool.database.manager import DatabaseManager
from shadowtool.configuration.manager import ConfigManager


@click.group()
def cli():
    pass


@cli.command()
def test_config():
    print("Hello")
    cm = ConfigManager('test-config.toml')
    dm = DatabaseManager(cm.db_config)
    print(dm.engines)


if __name__ == '__main__':
    cli()
