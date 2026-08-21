import threading
import time
import uuid
from collections import Counter
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
            if state.transport is None:
                state.transport = transport
        self.send_if_ready(invocation_id)

    def mark_start(self, invocation_id: str) -> None:
        with self._lock:
            self._state(invocation_id)

    def is_closed(self, invocation_id: str) -> bool:
        with self._lock:
            state = self._states.get(invocation_id)
            return state is not None and state.closed

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
                missing_expected = max(state.expected_result_count - len(top_level_results), 0)
                synthesized = [(None, status) for status in remaining_statuses[:missing_expected]]
                synthetic_ephemeral_count = max(len(remaining_statuses) - missing_expected, 0)
                selected_count = (
                    state.expected_result_count
                    + observed_ephemeral_count
                    + synthetic_ephemeral_count
                )
                reconciled_results = top_level_results + synthesized + state.hook_results
                return (
                    reconciled_results,
                    selected_count,
                    state.expected_result_count,
                    len(top_level_results) + len(synthesized) >= state.expected_result_count,
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
        # Keep connection-open off the network path.
        with self._lock:
            if not self._ready_to_send(self._states.get(invocation_id)):
                return
        thread = threading.Thread(
            target=self._drain,
            args=(invocation_id,),
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
        if state.post_parse is not None and not state.post_parse_sent:
            return True
        return (
            state.post_run is not None
            and not state.post_run_sent
            and (state.post_parse_terminal or state.post_parse is None)
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

    def _drain(self, invocation_id: str) -> None:
        # Serialize phases without holding the lock during sends.
        while True:
            with self._lock:
                state = self._states.get(invocation_id)
                if state is None or state.closed or state.sending:
                    return
                transport = state.transport
                if transport is None or transport.header_factory is None:
                    return

                phase = None
                payload = None
                event_id = None
                if state.post_parse is not None and not state.post_parse_sent:
                    phase = "post_parse"
                    payload = state.post_parse
                    event_id = state.post_parse_event_id
                    state.post_parse_sent = True
                elif (
                    state.post_run is not None
                    and not state.post_run_sent
                    and (state.post_parse_terminal or state.post_parse is None)
                ):
                    phase = "post_run"
                    payload = state.post_run
                    event_id = state.post_run_event_id
                    state.post_run_sent = True
                if payload is None or event_id is None:
                    return

                state.sending = True
                host = transport.host
                header_factory = transport.header_factory
                workspace_id = transport.workspace_id

            self._send(host, payload, event_id, header_factory, workspace_id)

            with self._lock:
                state = self._states.get(invocation_id)
                if state is None or state.closed:
                    return
                state.sending = False
                if phase == "post_parse":
                    state.post_parse_terminal = True

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
            state.post_run = None
            state.transport = None
            state.node_results.clear()
            state.hook_results.clear()
            state.end_run_statuses = None
            state.end_run_success = None
            state.fail_fast_triggered = False
            state.ephemeral_ids.clear()


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
