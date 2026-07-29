import json
import time
import uuid
from typing import Any, Optional

from dbt.adapters.databricks.telemetry.models import ConnectionLog

DRIVER_NAME = "dbt-databricks"
CLIENT_APP_NAME = "dbt"


def encode_event(log: ConnectionLog, workspace_id: Optional[int] = None) -> str:
    """Encode one ConnectionLog as a TelemetryFrontendLog JSON string."""
    sql_driver_log: dict[str, Any] = {
        "session_id": log.session_id,
        "system_configuration": {
            "driver_name": DRIVER_NAME,
            "driver_version": log.dbt_databricks_version,
            "client_app_name": CLIENT_APP_NAME,
        },
        "driver_connection_params": {
            "http_path": log.http_path,
        },
    }

    frontend_log: dict[str, Any] = {
        "frontend_log_event_id": str(uuid.uuid4()),
        "context": {
            "client_context": {
                "timestamp_millis": int(time.time() * 1000),
                "user_agent": f"{DRIVER_NAME}/{log.dbt_databricks_version}",
            }
        },
        "entry": {"sql_driver_log": sql_driver_log},
        "workspace_id": workspace_id,
    }
    return json.dumps(frontend_log)


def encode_request(log: ConnectionLog, workspace_id: Optional[int] = None) -> dict[str, Any]:
    """Build the TelemetryRequest body wrapping a single connection log."""
    return {
        "uploadTime": int(time.time() * 1000),
        "items": [],
        "protoLogs": [encode_event(log, workspace_id)],
    }
