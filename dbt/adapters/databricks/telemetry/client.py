from typing import Any, Callable, Optional

import requests

from dbt.adapters.databricks.credentials import BearerAuth

TELEMETRY_AUTHENTICATED_PATH = "/telemetry-ext"
TELEMETRY_UNAUTHENTICATED_PATH = "/telemetry-unauth"

_TIMEOUT_SECONDS = 10

HeaderFactory = Callable[[], dict[str, str]]


def _normalize_host(host: str) -> str:
    host = host.rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


def send(
    host: Optional[str],
    body: dict[str, Any],
    header_factory: Optional[HeaderFactory] = None,
    workspace_id: Optional[int] = None,
) -> bool:
    if not host:
        return False

    try:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        path = TELEMETRY_AUTHENTICATED_PATH
        if header_factory is not None:
            auth = BearerAuth(header_factory)
        else:
            auth = None
            path = TELEMETRY_UNAUTHENTICATED_PATH

        if workspace_id is not None:
            headers["x-databricks-org-id"] = str(workspace_id)

        url = _normalize_host(host) + path

        response = requests.post(
            url,
            json=body,
            headers=headers,
            auth=auth,
            timeout=_TIMEOUT_SECONDS,
        )

        if response.status_code // 100 != 2:
            return False
        try:
            ack = response.json()
        except ValueError:
            return False
        return ack.get("numProtoSuccess", 0) >= 1 and not ack.get("errors")
    except Exception:
        return False
