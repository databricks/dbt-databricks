#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf8') as f:
    long_description = f.read()


package_name = "dbt-databricks"


# get this from a separate file
def _dbt_databricks_version():
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'databricks', '__version__.py'
    )
    _version_pattern = r'''version\s*=\s*["'](.+)["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)


package_version = _dbt_databricks_version()
description = """The Databricks plugin for dbt (data build tool)"""

dbt_version = '0.21.1'
# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.21.1 vs 0.21.1a1, 0.21.1.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_version}'
    )

setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author='Databricks',
    author_email='feedback@databricks.com',
    url='https://github.com/databricks/dbt-databricks',

    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        f'dbt-core=={dbt_version}',
        'databricks-sql-connector>=0.9.0',
    ],
)
