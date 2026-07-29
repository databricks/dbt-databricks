"""Build the initial connection telemetry log from connection facts."""

from importlib.metadata import version as _pkg_version
from typing import Optional

from databricks.sql import __version__ as dbsql_version
from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.telemetry.models import ConnectionLog


def build_connection_log(
    session_id: Optional[str] = None,
    http_path: Optional[str] = None,
    is_cluster: Optional[bool] = None,
) -> ConnectionLog:
    """Assemble the connection log; only non-sensitive metadata."""
    compute_type = None
    if is_cluster is not None:
        compute_type = "cluster" if is_cluster else "sql_warehouse"
    return ConnectionLog(
        dbt_databricks_version=__version__,
        dbt_core_version=_pkg_version("dbt-core"),
        databricks_sql_connector_version=dbsql_version,
        session_id=session_id,
        http_path=http_path,
        compute_type=compute_type,
    )
