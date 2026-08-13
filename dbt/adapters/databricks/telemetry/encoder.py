"""Encode an event as a FrontendLog telemetry request via the first-class
dbt_databricks_telemetry_log field; dataclass field names match the proto,
so asdict yields the proto JSON directly.
"""

import dataclasses
import json
import time
from typing import Any, Optional

from dbt.adapters.databricks.telemetry.models import TelemetryLog

DRIVER_NAME = "dbt-databricks"


def _coerce_workspace_id(workspace_id: Optional[Any]) -> Optional[int]:
    try:
        return int(workspace_id) if workspace_id is not None else None
    except (TypeError, ValueError):
        return None


def encode_frontend_log(
    log: TelemetryLog,
    frontend_log_event_id: str,
    workspace_id: Optional[Any] = None,
) -> str:
    """Encode one TelemetryLog as a FrontendLog JSON string."""
    entry = {"dbt_databricks_telemetry_log": dataclasses.asdict(log)}
    frontend_log: dict[str, Any] = {
        "frontend_log_event_id": frontend_log_event_id,
        "context": {
            "client_context": {
                "timestamp_millis": int(time.time() * 1000),
                "user_agent": f"{DRIVER_NAME}/{log.adapter_version}",
            }
        },
        "entry": entry,
    }
    coerced = _coerce_workspace_id(workspace_id)
    if coerced is not None:
        frontend_log["workspace_id"] = coerced
    return json.dumps(frontend_log)


def encode_request(
    log: TelemetryLog,
    frontend_log_event_id: str,
    workspace_id: Optional[Any] = None,
) -> dict[str, Any]:
    """Build the TelemetryRequest body wrapping a single event."""
    return {
        "uploadTime": int(time.time() * 1000),
        "items": [],
        "protoLogs": [encode_frontend_log(log, frontend_log_event_id, workspace_id)],
    }
