import threading
import time
import uuid
from typing import Any, Callable, Optional

from dbt.adapters.databricks.telemetry import client, encoder
from dbt.adapters.databricks.telemetry.models import TelemetryLog

HeaderFactory = Callable[[], dict[str, str]]


class Transport:
    """Reusable transport with a redacted representation."""

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
        "transport",
        "post_parse_event_id",
        "post_parse_sent",
        "post_parse_timestamp_millis",
        "sending",
        "closed",
    )

    def __init__(self) -> None:
        self.post_parse: Optional[TelemetryLog] = None
        self.transport: Optional[Transport] = None
        self.post_parse_event_id: str = str(uuid.uuid4())
        self.post_parse_sent: bool = False
        self.post_parse_timestamp_millis: Optional[int] = None
        self.sending: bool = False
        self.closed: bool = False


class Coordinator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, _InvocationState] = {}
        self._threads_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

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
            if state.post_parse is None:
                state.post_parse = payload
                state.post_parse_timestamp_millis = int(time.time() * 1000)
        self.send_if_ready(invocation_id)

    def set_transport(self, invocation_id: str, transport: Transport) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is None:
                return
            if state.transport is None:
                state.transport = transport
        self.send_if_ready(invocation_id)

    def mark_start(self, invocation_id: str) -> None:
        with self._lock:
            self._states = {
                key: state
                for key, state in self._states.items()
                if key == invocation_id or not state.closed
            }
            self._state(invocation_id)

    def needs_post_parse(self, invocation_id: str) -> bool:
        with self._lock:
            state = self._states.get(invocation_id)
            return state is None or (not state.closed and state.post_parse is None)

    def send_if_ready(self, invocation_id: str) -> None:
        # Keep connection-open off the network path. Claim send inputs under
        # the lock so a later close() cannot cancel already-scheduled work.
        with self._lock:
            claimed = self._claim_send(self._states.get(invocation_id))
            if claimed is None:
                return
        thread = threading.Thread(
            target=self._send_claimed,
            args=(invocation_id, *claimed),
            name="dbt-telemetry-send",
            daemon=True,
        )
        with self._threads_lock:
            self._threads = [t for t in self._threads if t.is_alive()]
            self._threads.append(thread)
        thread.start()

    def _ready_to_send(self, state: Optional[_InvocationState]) -> bool:
        if state is None or state.closed or state.sending:
            return False
        transport = state.transport
        if transport is None or transport.header_factory is None:
            return False
        return state.post_parse is not None and not state.post_parse_sent

    def _claim_send(
        self, state: Optional[_InvocationState]
    ) -> Optional[
        tuple[Optional[str], TelemetryLog, str, HeaderFactory, Optional[Any], Optional[int]]
    ]:
        if not self._ready_to_send(state) or state is None or state.transport is None:
            return None
        header_factory = state.transport.header_factory
        if state.post_parse is None or header_factory is None:
            return None
        transport = state.transport
        payload = state.post_parse
        event_id = state.post_parse_event_id
        event_timestamp_millis = state.post_parse_timestamp_millis
        state.post_parse_sent = True
        state.sending = True
        return (
            transport.host,
            payload,
            event_id,
            header_factory,
            transport.workspace_id,
            event_timestamp_millis,
        )

    def flush(self, timeout: Optional[float] = None) -> None:
        if timeout is None:
            timeout = float(client._TIMEOUT_SECONDS * 2)
        deadline = time.monotonic() + timeout
        while True:
            with self._threads_lock:
                self._threads = [t for t in self._threads if t.is_alive()]
                pending = list(self._threads)
            if not pending:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            pending[0].join(remaining)

    def _send_claimed(
        self,
        invocation_id: str,
        host: Optional[str],
        payload: TelemetryLog,
        event_id: str,
        header_factory: HeaderFactory,
        workspace_id: Optional[Any],
        event_timestamp_millis: Optional[int],
    ) -> None:
        self._send(host, payload, event_id, header_factory, workspace_id, event_timestamp_millis)
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            state.sending = False

    def _send(
        self,
        host: Optional[str],
        payload: TelemetryLog,
        event_id: str,
        header_factory: Optional[HeaderFactory],
        workspace_id: Optional[Any],
        event_timestamp_millis: Optional[int] = None,
    ) -> None:
        try:
            body = encoder.encode_request(
                payload,
                event_id,
                workspace_id=workspace_id,
                event_timestamp_millis=event_timestamp_millis,
            )
            client.send(host, body, header_factory=header_factory, workspace_id=workspace_id)
        except Exception:  # pragma: no cover - best-effort
            return

    def close(self, invocation_id: str) -> None:
        with self._lock:
            # Reject late callbacks and clear sensitive state.
            state = self._states.get(invocation_id)
            if state is None:
                state = _InvocationState()
                self._states[invocation_id] = state
            state.closed = True
            state.post_parse = None
            state.transport = None


_COORDINATOR = Coordinator()


def coordinator() -> Coordinator:
    return _COORDINATOR
