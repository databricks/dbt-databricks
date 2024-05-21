#!/usr/bin/env python
import os
import sys

# require python 3.8 or newer
if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)


# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" and try again')
    sys.exit(1)


# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), "r", encoding="utf8") as f:
    long_description = f.read()


# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version() -> str:
    _version_path = os.path.join(this_directory, "dbt", "adapters", "databricks", "__version__.py")
    try:
        exec(open(_version_path).read())
        return locals()["version"]
    except IOError:
        print("Failed to load dbt-databricks version file for packaging.", file=sys.stderr)
        sys.exit(-1)


package_name = "dbt-databricks"
package_version = _get_plugin_version()
description = """The Databricks adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Databricks",
    author_email="feedback@databricks.com",
    url="https://github.com/databricks/dbt-databricks",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-spark~=1.8.0rc1",
        "dbt-core~=1.8.0rc2",
        "dbt-adapters~=1.1.1",
        "databricks-sql-connector>=3.1.0, <3.2.0",
        "databricks-sdk==0.17.0",
        "keyring>=23.13.0",
        "pandas<2.2.0",
        "protobuf<5.0.0",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
