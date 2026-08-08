"""Thin hooks called from the connection manager.

On the first successful connection we build one initial connection log and POST
it immediately, using the connection's host + auth.
"""

from typing import Optional

from dbt.adapters.databricks import telemetry
from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)
from dbt.adapters.databricks.logging import logger

_sent = False


def on_connection_open(
    credentials: Optional[DatabricksCredentials],
    credentials_manager: Optional[DatabricksCredentialManager],
    session_id: Optional[str] = None,
    http_path: Optional[str] = None,
    is_cluster: Optional[bool] = None,
) -> None:
    """Send a single initial connection telemetry log on the first opted-in connection."""
    global _sent
    if _sent:
        return
    if not telemetry.is_enabled(credentials):
        return
    if credentials_manager is None:
        return
    try:
        _sent = True
        telemetry.send_connection_log(
            host=credentials_manager.host,
            header_factory=credentials_manager.header_factory,
            workspace_id=getattr(credentials_manager, "workspace_id", None),
            session_id=session_id,
            http_path=http_path,
            is_cluster=is_cluster,
        )
    except Exception as e:
        logger.debug(f"dbt telemetry: on_connection_open failed (ignored): {e}")
