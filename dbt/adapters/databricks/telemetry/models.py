"""Event model for the dbt-databricks initial connection telemetry log."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionLog:
    dbt_databricks_version: str
    dbt_core_version: str
    databricks_sql_connector_version: str
    session_id: Optional[str] = None
    http_path: Optional[str] = None
    compute_type: Optional[str] = None  # "cluster" | "sql_warehouse"
