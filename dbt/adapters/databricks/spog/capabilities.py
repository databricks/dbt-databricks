"""Detect whether the installed connector and SDK support SPOG.

Floors on databricks-sql-connector and databricks-sdk are intentionally kept
low so users on pre-SPOG versions continue working unchanged. These predicates
let the rest of the adapter decide whether the SPOG code path is reachable
in the current environment.
"""

from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from packaging.version import Version

from databricks.sdk.core import Config

CONNECTOR_SPOG_MIN = Version("4.2.6")


@cache
def connector_supports_spog() -> bool:
    """True iff the installed databricks-sql-connector is >= 4.2.6.

    Uses packaging.version.Version for PEP-440-correct ordering across
    alpha / post / dev release suffixes. Cached for the lifetime of the
    process.
    """
    try:
        return Version(_pkg_version("databricks-sql-connector")) >= CONNECTOR_SPOG_MIN
    except PackageNotFoundError:
        return False


@cache
def sdk_supports_workspace_id() -> bool:
    """True iff the installed databricks-sdk exposes a `workspace_id` attribute on Config.

    The SDK's Config uses **kwargs + ConfigAttribute descriptors at the class
    level, so inspect.signature(Config) does NOT list workspace_id even on
    SPOG-capable versions. Use hasattr against the class to feature-detect
    the descriptor. workspace_id was introduced as a Config attribute in
    databricks-sdk v0.103.0.
    """
    return hasattr(Config, "workspace_id")
