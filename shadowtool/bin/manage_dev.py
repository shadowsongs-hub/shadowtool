import click


@click.group
def cli():
    pass


@cli.command
def test_config():
    print("Hello")


if __name__ == '__main__':
    cli()
