#!/usr/bin/env python
from setuptools import setup, find_packages
from shadowtool import __version__


# Load the package's __version__.py module as a dictionary.
setup(
    name='shadowtool',
    version=__version__,
    description='A personal developement toolkit for Shadowsong',
    url='https://github.com/shadowsongs-hub/shadowtool',
    author='Shadowsong27',
    author_email='syk950527@gmail.com',
    license='',
    classifiers=[
        'Development Status :: 1 - Planning',
    ],
    keywords='',
    packages=find_packages(exclude=['test*']),
    install_requires=[
        'toml==0.10.0',
        'attrs==19.1.0',
        'click==7.0',
        'psycopg2-binary==2.8.3',
        'sqlalchemy==1.3.0'
    ],
    package_data={},
    data_files=[],
    entry_points="""
        [console_scripts]
        shadowtool=shadowtool.bin.manage_dev:cli
    """,
)
