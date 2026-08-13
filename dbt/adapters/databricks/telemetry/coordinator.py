"""Per-invocation telemetry coordinator.

Keyed by dbt's invocation_id so sequential dbtRunner runs never cross-pair
manifest and transport. Parse and first connection open may arrive in either
order; POST_PARSE sends once both payload and an authenticated transport exist.
Best-effort: failures are swallowed, never changing dbt command success;
network calls happen outside the lock.
"""

import threading
import uuid
from typing import Any, Callable, Optional

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry import client, encoder
from dbt.adapters.databricks.telemetry.models import TelemetryLog

HeaderFactory = Callable[[], dict[str, str]]


class Transport:
    """Opaque, non-serializable reusable-transport handle. Never enters a
    payload or debug output, so it offers no dict conversion and redacts repr.
    """

    __slots__ = ("host", "header_factory", "workspace_id")

    def __init__(
        self,
        host: Optional[str],
        header_factory: Optional[HeaderFactory],
        workspace_id: Optional[Any],
    ) -> None:
        self.host = host
        self.header_factory = header_factory
        self.workspace_id = workspace_id

    def __repr__(self) -> str:  # pragma: no cover - defensive redaction
        return "<dbt telemetry Transport (redacted)>"


class _InvocationState:
    __slots__ = ("payload", "transport", "event_id", "post_parse_sent", "closed")

    def __init__(self) -> None:
        self.payload: Optional[TelemetryLog] = None
        self.transport: Optional[Transport] = None
        # Stable frontend-log id; reused across retries.
        self.event_id: str = str(uuid.uuid4())
        self.post_parse_sent: bool = False
        self.closed: bool = False


class Coordinator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, _InvocationState] = {}

    def _state(self, invocation_id: str) -> Optional[_InvocationState]:
        state = self._states.get(invocation_id)
        if state is None:
            state = _InvocationState()
            self._states[invocation_id] = state
        elif state.closed:
            return None
        return state

    def set_post_parse(self, invocation_id: str, payload: TelemetryLog) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is None:
                return
            # Idempotent: repeated parse callbacks keep the first payload.
            if state.payload is None:
                state.payload = payload
        self.send_if_ready(invocation_id)

    def set_transport(self, invocation_id: str, transport: Transport) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is None:
                return
            # Only the first reusable transport is retained.
            if state.transport is None:
                state.transport = transport
        self.send_if_ready(invocation_id)

    def send_if_ready(self, invocation_id: str) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed or state.post_parse_sent:
                return
            if state.payload is None or state.transport is None:
                return
            # Authenticated endpoint only: no auth headers means not ready.
            if state.transport.header_factory is None:
                return
            # Claim the send while holding the lock; do the network call outside.
            state.post_parse_sent = True
            payload = state.payload
            transport = state.transport
            event_id = state.event_id

        try:
            body = encoder.encode_request(payload, event_id, workspace_id=transport.workspace_id)
            client.send(
                transport.host,
                body,
                header_factory=transport.header_factory,
                workspace_id=transport.workspace_id,
            )
        except Exception as e:  # pragma: no cover - best-effort
            logger.debug(f"dbt telemetry: post-parse send failed (ignored): {e}")

    def close(self, invocation_id: str) -> None:
        with self._lock:
            # Leave a closed tombstone so a late callback cannot recreate state.
            state = self._states.get(invocation_id)
            if state is None:
                state = _InvocationState()
                self._states[invocation_id] = state
            state.closed = True


# Process-wide singleton.
_COORDINATOR = Coordinator()


def coordinator() -> Coordinator:
    return _COORDINATOR
