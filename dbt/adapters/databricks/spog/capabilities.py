"""Detect whether the installed connector and SDK support SPOG."""

from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from packaging.version import Version

from databricks.sdk.core import Config

CONNECTOR_SPOG_MIN = Version("4.2.6")


@cache
def connector_supports_spog() -> bool:
    """True iff the installed databricks-sql-connector is >= 4.2.6."""
    try:
        return Version(_pkg_version("databricks-sql-connector")) >= CONNECTOR_SPOG_MIN
    except PackageNotFoundError:
        return False


@cache
def sdk_supports_workspace_id() -> bool:
    """True iff the installed databricks-sdk exposes a `workspace_id` attribute on Config."""
    return hasattr(Config, "workspace_id")
