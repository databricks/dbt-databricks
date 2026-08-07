"""Discovery probe for /.well-known/databricks-config.

Lets the adapter classify a host as SPOG (host_type='unified') or legacy.
Probe is one-shot per host per process; result is cached.
"""

import random
import time
from dataclasses import dataclass
from functools import cache
from typing import Optional

import requests

from dbt.adapters.databricks.logging import logger


@dataclass(frozen=True)
class HostMetadata:
    """Subset of /.well-known/databricks-config the adapter consumes."""

    host_type: Optional[str]
    account_id: Optional[str] = None


@cache
def probe_host(host: str) -> HostMetadata:
    """Probe https://{host}/.well-known/databricks-config. Cached per host.

    Retries up to 3 attempts with exponential backoff (~0.5s, 1s with jitter).
    On exhaustion, logs a WARN and returns HostMetadata(host_type=None) so the
    caller falls back to the legacy code path. Failure is never fatal.
    """
    url = f"https://{host}/.well-known/databricks-config"
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            body = resp.json()
            return HostMetadata(
                host_type=body.get("host_type"),
                account_id=body.get("account_id"),
            )
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < 2:
                time.sleep((0.5 * (2**attempt)) + random.random() * 0.25)

    logger.info(
        f"SPOG discovery probe to {url!r} failed after 3 attempts (last error: {last_exc}). "
        f"Proceeding as a non-SPOG host. If {host} is a SPOG (unified) workspace, routing "
        f"errors may follow; verify network reachability to /.well-known/databricks-config."
    )
    return HostMetadata(host_type=None)
