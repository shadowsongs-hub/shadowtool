import click

from shadowtool.database.manager import DatabaseManager
from shadowtool.configuration.manager import ConfigManager


@click.group()
def cli():
    pass


@cli.command()
def test_config():
    cm = ConfigManager('test-config.toml')
    dm = DatabaseManager(cm.db_config)
    hook = dm.hooks['real_dev']
    result = hook.execute_query(
        query="""
        SELECT
            *
        FROM public.test
        """
    )

    print(result)


if __name__ == '__main__':
    cli()
