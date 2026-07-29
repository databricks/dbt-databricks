from typing import Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials

ENABLE_FLAG = "enable_dbt_telemetry"


def is_enabled(credentials: Optional[DatabricksCredentials]) -> bool:
    """True only when the user explicitly opted in on this target."""
    if credentials is None:
        return False
    params = credentials.connection_parameters or {}
    return bool(params.get(ENABLE_FLAG, False))
