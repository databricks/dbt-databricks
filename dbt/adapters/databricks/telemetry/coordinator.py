"""Per-invocation telemetry coordinator.

Keyed by dbt's invocation_id so sequential dbtRunner runs never cross-pair
manifest and transport. Parse and first connection open may arrive in either
order; POST_PARSE sends once both payload and an authenticated transport exist.
POST_RUN sends after the POST_PARSE attempt is terminal, reusing the same
transport. Best-effort: failures are swallowed, never changing dbt command
success; network calls happen outside the lock.
"""

import threading
import time
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
    __slots__ = (
        "post_parse",
        "post_run",
        "transport",
        # Separate stable frontend-log ids per phase; reused across retries.
        "post_parse_event_id",
        "post_run_event_id",
        "post_parse_sent",
        "post_run_sent",
        "start_monotonic",
        # Per-node (resource_type, status) captured from run events.
        "node_results",
        "results_captured",
        "closed",
    )

    def __init__(self) -> None:
        self.post_parse: Optional[TelemetryLog] = None
        self.post_run: Optional[TelemetryLog] = None
        self.transport: Optional[Transport] = None
        self.post_parse_event_id: str = str(uuid.uuid4())
        self.post_run_event_id: str = str(uuid.uuid4())
        self.post_parse_sent: bool = False
        self.post_run_sent: bool = False
        self.start_monotonic: float = time.monotonic()
        self.node_results: list = []
        self.results_captured: bool = False
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
            # Idempotent: repeated callbacks keep the first payload.
            if state.post_parse is None:
                state.post_parse = payload
        self.send_if_ready(invocation_id)

    def set_post_run(self, invocation_id: str, payload: TelemetryLog) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is None:
                return
            if state.post_run is None:
                state.post_run = payload
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

    def mark_start(self, invocation_id: str) -> None:
        # Create the state early so its start timestamp predates parse.
        with self._lock:
            self._state(invocation_id)

    def begin_result_capture(self, invocation_id: str) -> None:
        # Marks that node results are being captured, so POST_RUN can report
        # result_aggregates_available even when the run produced zero nodes.
        with self._lock:
            state = self._state(invocation_id)
            if state is not None:
                state.results_captured = True

    def record_node_result(self, invocation_id: str, resource_type: str, status: str) -> None:
        # Called from run-event callbacks on worker threads.
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            state.node_results.append((resource_type, status))

    def result_snapshot(self, invocation_id: str) -> tuple:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return [], False
            return list(state.node_results), state.results_captured

    def elapsed_ms(self, invocation_id: str) -> int:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return 0
            return int((time.monotonic() - state.start_monotonic) * 1000)

    def send_if_ready(self, invocation_id: str) -> None:
        to_send: list = []
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            transport = state.transport
            # Authenticated endpoint only: no transport/headers means not ready.
            if transport is None or transport.header_factory is None:
                return
            if state.post_parse is not None and not state.post_parse_sent:
                state.post_parse_sent = True
                to_send.append((state.post_parse, state.post_parse_event_id))
            # POST_RUN waits until the POST_PARSE attempt is terminal.
            post_parse_terminal = state.post_parse_sent or state.post_parse is None
            if state.post_run is not None and not state.post_run_sent and post_parse_terminal:
                state.post_run_sent = True
                to_send.append((state.post_run, state.post_run_event_id))
            host = transport.host
            header_factory = transport.header_factory
            workspace_id = transport.workspace_id

        for payload, event_id in to_send:
            self._send(host, payload, event_id, header_factory, workspace_id)

    def _send(
        self,
        host: Optional[str],
        payload: TelemetryLog,
        event_id: str,
        header_factory: Optional[HeaderFactory],
        workspace_id: Optional[Any],
    ) -> None:
        try:
            body = encoder.encode_request(payload, event_id, workspace_id=workspace_id)
            client.send(host, body, header_factory=header_factory, workspace_id=workspace_id)
        except Exception as e:  # pragma: no cover - best-effort
            logger.debug(f"dbt telemetry: send failed (ignored): {e}")

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
