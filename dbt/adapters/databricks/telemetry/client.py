import json
from typing import Any, Callable, Optional

import requests

from dbt.adapters.databricks.logging import logger

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
        logger.debug("dbt telemetry: no host available; skipping send")
        return False

    try:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        path = TELEMETRY_AUTHENTICATED_PATH
        if header_factory is not None:
            try:
                headers.update(header_factory())
            except Exception as e:
                logger.debug(f"dbt telemetry: failed to build auth headers: {e}")
                return False
        else:
            path = TELEMETRY_UNAUTHENTICATED_PATH

        if workspace_id is not None:
            headers["x-databricks-org-id"] = str(workspace_id)

        url = _normalize_host(host) + path

        logger.debug(f"dbt telemetry: log = {json.dumps(body)}")
        logger.debug(f"dbt telemetry: endpoint = {url}")

        response = requests.post(url, json=body, headers=headers, timeout=_TIMEOUT_SECONDS)

        body_preview = response.text if len(response.text) <= 500 else response.text[:500] + "…"
        logger.debug(f"dbt telemetry: response = [{response.status_code}] {body_preview}")

        if response.status_code // 100 != 2:
            logger.debug(f"dbt telemetry: not accepted (status {response.status_code})")
            return False
        try:
            ack = response.json()
        except ValueError:
            logger.debug("dbt telemetry: not accepted (non-JSON response)")
            return False
        accepted = ack.get("numProtoSuccess", 0) >= 1 and not ack.get("errors")
        if not accepted:
            logger.debug(f"dbt telemetry: not accepted (ack: {ack})")
        return accepted
    except Exception as e:
        logger.debug(f"dbt telemetry: send failed (ignored): {e}")
        return False
