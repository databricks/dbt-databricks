import threading
import time
import uuid
from collections import Counter
from typing import Any, Callable, Optional

from dbt.adapters.databricks.telemetry import client, encoder
from dbt.adapters.databricks.telemetry.models import TelemetryLog

HeaderFactory = Callable[[], dict[str, str]]
_ClaimedSend = tuple[
    str, Optional[str], TelemetryLog, str, HeaderFactory, Optional[Any], Optional[int]
]


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
        "post_run",
        "transport",
        "post_parse_event_id",
        "post_run_event_id",
        "post_parse_sent",
        "post_parse_terminal",
        "post_run_sent",
        "sending",
        "start_monotonic",
        "node_results",
        "hook_results",
        "end_run_statuses",
        "end_run_success",
        "fail_fast_triggered",
        "expected_result_count",
        "ephemeral_ids",
        "results_captured",
        "closed",
        "post_parse_timestamp_millis",
        "post_run_timestamp_millis",
    )

    def __init__(self) -> None:
        self.post_parse: Optional[TelemetryLog] = None
        self.post_run: Optional[TelemetryLog] = None
        self.transport: Optional[Transport] = None
        self.post_parse_event_id: str = str(uuid.uuid4())
        self.post_run_event_id: str = str(uuid.uuid4())
        self.post_parse_sent: bool = False
        self.post_parse_terminal: bool = False
        self.post_run_sent: bool = False
        self.sending: bool = False
        self.start_monotonic: float = time.monotonic()
        self.node_results: list = []
        self.hook_results: list = []
        self.end_run_statuses: Optional[list] = None
        self.end_run_success: Optional[bool] = None
        self.fail_fast_triggered: bool = False
        self.expected_result_count: int = 0
        self.ephemeral_ids: set[str] = set()
        self.results_captured: bool = False
        self.closed: bool = False
        self.post_parse_timestamp_millis: Optional[int] = None
        self.post_run_timestamp_millis: Optional[int] = None


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

    def set_post_run(self, invocation_id: str, payload: TelemetryLog) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is None:
                return
            if state.post_run is None:
                state.post_run = payload
                state.post_run_timestamp_millis = int(time.time() * 1000)
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
                if key == invocation_id or self._retain_closed_state(state)
            }
            self._state(invocation_id)

    def is_closed(self, invocation_id: str) -> bool:
        with self._lock:
            state = self._states.get(invocation_id)
            return state is not None and state.closed

    def is_active(self, invocation_id: str) -> bool:
        with self._lock:
            state = self._states.get(invocation_id)
            return state is not None and not state.closed

    def needs_post_parse(self, invocation_id: str) -> bool:
        with self._lock:
            state = self._states.get(invocation_id)
            return state is None or (not state.closed and state.post_parse is None)

    def record_node_result(self, invocation_id: str, unique_id: str, status: str) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            # Error events may precede NodeFinished for the same node.
            for index, (observed_id, _) in enumerate(state.node_results):
                if str(observed_id) == str(unique_id):
                    state.node_results[index] = (unique_id, status)
                    break
            else:
                state.node_results.append((unique_id, status))
            state.results_captured = True

    def record_hook_result(self, invocation_id: str, status: str) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            state.hook_results.append(("operation", status))
            state.results_captured = True

    def record_end_run(
        self, invocation_id: str, statuses: list, success: Optional[bool] = None
    ) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            state.end_run_statuses = list(statuses)
            state.end_run_success = success
            state.results_captured = True

    def mark_fail_fast_triggered(self, invocation_id: str) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed or state.end_run_statuses is not None:
                return
            state.fail_fast_triggered = True

    def outcome_snapshot(self, invocation_id: str) -> tuple[Optional[bool], bool]:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return None, False
            return state.end_run_success, state.fail_fast_triggered

    def record_expected_count(self, invocation_id: str, count: int) -> None:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            state.expected_result_count = count

    def record_ephemeral_ids(self, invocation_id: str, unique_ids: set[str]) -> None:
        with self._lock:
            state = self._state(invocation_id)
            if state is not None:
                state.ephemeral_ids = set(unique_ids)

    def result_snapshot(self, invocation_id: str) -> tuple:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return [], 0, 0, False, False
            observed_ids = {str(uid) for uid, _ in state.node_results}
            observed_ephemeral_count = len(observed_ids.intersection(state.ephemeral_ids))
            top_level_results = [
                result for result in state.node_results if str(result[0]) not in state.ephemeral_ids
            ]
            if state.end_run_statuses is not None:
                # EndRunResult statuses have no unique IDs.
                remaining = Counter(_status_key(status) for status in state.end_run_statuses)
                typed_results = top_level_results + state.hook_results
                for _, status in typed_results:
                    key = _status_key(status)
                    if remaining[key] > 0:
                        remaining[key] -= 1
                remaining_statuses = [
                    status for status in state.end_run_statuses if _take_status(remaining, status)
                ]
                expected_count = max(state.expected_result_count, len(top_level_results))
                missing_expected = max(expected_count - len(top_level_results), 0)
                synthesized = [(None, status) for status in remaining_statuses[:missing_expected]]
                leftover_after_expected = max(len(remaining_statuses) - missing_expected, 0)
                synthetic_ephemeral_count = max(
                    leftover_after_expected - observed_ephemeral_count, 0
                )
                selected_count = (
                    expected_count + observed_ephemeral_count + synthetic_ephemeral_count
                )
                reconciled_results = top_level_results + synthesized + state.hook_results
                return (
                    reconciled_results,
                    selected_count,
                    expected_count,
                    len(top_level_results) + len(synthesized) >= expected_count,
                    True,
                )
            # Missing ephemeral results make selection unknown.
            partial_selected_count: Optional[int]
            if not state.ephemeral_ids:
                partial_selected_count = state.expected_result_count
            elif observed_ephemeral_count == len(state.ephemeral_ids):
                partial_selected_count = state.expected_result_count + observed_ephemeral_count
            else:
                partial_selected_count = None
            return (
                top_level_results + state.hook_results,
                partial_selected_count,
                state.expected_result_count,
                False,
                False,
            )

    def elapsed_ms(self, invocation_id: str) -> int:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return 0
            elapsed_seconds = time.monotonic() - state.start_monotonic
            return int(max(elapsed_seconds, 0.0) * 1000)

    def send_if_ready(self, invocation_id: str) -> None:
        # Keep connection-open off the network path. Claim send inputs under
        # the lock so a later close() cannot cancel already-scheduled work.
        with self._lock:
            claimed = self._claim_sends(self._states.get(invocation_id))
            if not claimed:
                return
        thread = threading.Thread(
            target=self._send_claimed,
            args=(invocation_id, claimed),
            name="dbt-telemetry-send",
            daemon=True,
        )
        with self._threads_lock:
            self._threads = [t for t in self._threads if t.is_alive()]
            self._threads.append(thread)
        thread.start()

    def _ready_to_send(self, state: Optional[_InvocationState]) -> bool:
        if state is None or state.sending:
            return False
        return self._unclaimed_send_ready(state)

    def _unclaimed_send_ready(self, state: _InvocationState) -> bool:
        transport = state.transport
        if transport is None or transport.header_factory is None:
            return False
        if state.post_parse is not None and not state.post_parse_sent:
            return True
        return (
            state.post_run is not None
            and not state.post_run_sent
            and (state.post_parse_terminal or state.post_parse is None)
        )

    def _retain_closed_state(self, state: _InvocationState) -> bool:
        if not state.closed:
            return True
        return state.sending or self._unclaimed_send_ready(state)

    def _scrub(self, state: _InvocationState) -> None:
        state.post_parse = None
        state.post_run = None
        state.transport = None

    def _claim_sends(self, state: Optional[_InvocationState]) -> list[_ClaimedSend]:
        if not self._ready_to_send(state) or state is None or state.transport is None:
            return []
        header_factory = state.transport.header_factory
        if header_factory is None:
            return []
        transport = state.transport
        claimed: list[_ClaimedSend] = []
        if state.post_parse is not None and not state.post_parse_sent:
            state.post_parse_sent = True
            state.post_parse_terminal = True
            claimed.append(
                (
                    "post_parse",
                    transport.host,
                    state.post_parse,
                    state.post_parse_event_id,
                    header_factory,
                    transport.workspace_id,
                    state.post_parse_timestamp_millis,
                )
            )
        if (
            state.post_run is not None
            and not state.post_run_sent
            and (state.post_parse_terminal or state.post_parse is None)
        ):
            state.post_run_sent = True
            claimed.append(
                (
                    "post_run",
                    transport.host,
                    state.post_run,
                    state.post_run_event_id,
                    header_factory,
                    transport.workspace_id,
                    state.post_run_timestamp_millis,
                )
            )
        if claimed:
            state.sending = True
        return claimed

    def flush(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            timeout = float(client._TIMEOUT_SECONDS * 2)
        deadline = time.monotonic() + timeout
        while True:
            with self._threads_lock:
                self._threads = [t for t in self._threads if t.is_alive()]
                pending = list(self._threads)
            if not pending:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            pending[0].join(remaining)

    def _send_claimed(
        self,
        invocation_id: str,
        claimed: list[_ClaimedSend],
    ) -> None:
        while claimed:
            for (
                _phase,
                host,
                payload,
                event_id,
                header_factory,
                workspace_id,
                event_timestamp_millis,
            ) in claimed:
                self._send(
                    host,
                    payload,
                    event_id,
                    header_factory,
                    workspace_id,
                    event_timestamp_millis,
                )
            with self._lock:
                state = self._states.get(invocation_id)
                if state is None:
                    return
                state.sending = False
                claimed = self._claim_sends(state)
                if not claimed and state.closed:
                    self._scrub(state)

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
            # Reject late callbacks. Keep unsent payloads so an in-flight
            # POST_PARSE can still claim POST_RUN after this returns.
            state = self._states.get(invocation_id)
            if state is None:
                state = _InvocationState()
                self._states[invocation_id] = state
            state.closed = True
            state.node_results.clear()
            state.hook_results.clear()
            state.end_run_statuses = None
            state.end_run_success = None
            state.fail_fast_triggered = False
            state.ephemeral_ids.clear()
            if not state.sending and not self._unclaimed_send_ready(state):
                self._scrub(state)


def _status_key(status: Any) -> str:
    return str(status).strip().lower().replace("-", "_").replace(" ", "_")


def _take_status(remaining: Counter, status: Any) -> bool:
    key = _status_key(status)
    if remaining[key] <= 0:
        return False
    remaining[key] -= 1
    return True


_COORDINATOR = Coordinator()


def coordinator() -> Coordinator:
    return _COORDINATOR
