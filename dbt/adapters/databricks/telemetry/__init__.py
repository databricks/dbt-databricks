from typing import Optional

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry import builder, client, encoder
from dbt.adapters.databricks.telemetry.client import HeaderFactory
from dbt.adapters.databricks.telemetry.config import is_enabled

__all__ = ["is_enabled", "send_connection_log"]


def send_connection_log(
    host: Optional[str],
    header_factory: Optional[HeaderFactory] = None,
    workspace_id: Optional[int] = None,
    session_id: Optional[str] = None,
    http_path: Optional[str] = None,
    is_cluster: Optional[bool] = None,
) -> None:
    """Encode a single connection log and POST it."""
    try:
        log = builder.build_connection_log(
            session_id=session_id, http_path=http_path, is_cluster=is_cluster
        )
        body = encoder.encode_request(log, workspace_id=workspace_id)
        client.send(host, body, header_factory=header_factory, workspace_id=workspace_id)
    except Exception as e:  # pragma: no cover - defensive
        logger.debug(f"dbt telemetry: send_connection_log failed (ignored): {e}")
