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
from collections import Counter
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
        "post_parse_terminal",
        "post_run_sent",
        "sending",
        "start_monotonic",
        # (unique_id, status) results captured from run events.
        "node_results",  # per-node NodeFinished; interrupt fallback
        "hook_results",  # LogHookEndLine results; hooks do not fire NodeFinished
        "end_run_statuses",  # authoritative statuses (None until EndRunResult fires)
        "end_run_success",  # dbt-core's authoritative interpretation of the result
        "fail_fast_triggered",  # observed fail-fast result reporting before EndRunResult
        "expected_result_count",  # ConcurrencyLine node_count
        "ephemeral_ids",  # sanitized manifest lookup used only for selected count
        "results_captured",
        "telemetry_delivery_seconds",  # excluded from invocation_duration_ms
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
        self.telemetry_delivery_seconds: float = 0.0
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

    def record_node_result(self, invocation_id: str, unique_id: str, status: str) -> None:
        # Per-node NodeFinished, from run-event callbacks on worker threads.
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None or state.closed:
                return
            # Some error/skip paths have a dedicated event in addition to a
            # NodeFinished event. Keep one terminal observation per resource.
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
        # EndRunResult includes fail-fast synthesized skips, but its RunResultMsg
        # entries have no unique_id. Reconcile these statuses with typed events in
        # result_snapshot.
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
        # Returns (results, selected_count, expected_count, coverage_complete,
        # results_captured).
        # EndRunResult is authoritative and complete; NodeFinished is the partial
        # interrupt fallback, so coverage is only complete when EndRunResult fired.
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
                # Remove statuses already attributable to NodeFinished and hook
                # events. Any expected-result remainder is a synthesized result
                # whose type cannot be recovered from EndRunResult.
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
            # ConcurrencyLine gives an exact non-ephemeral population. Without
            # EndRunResult, however, unobserved ephemerals may be selected or may
            # simply be outside selection. Do not report a lower bound as exact.
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
                # Public run events are not dbt-core's task-published partial
                # snapshot. Be conservative on every path lacking EndRunResult.
                False,
            )

    def elapsed_ms(self, invocation_id: str) -> int:
        with self._lock:
            state = self._states.get(invocation_id)
            if state is None:
                return 0
            elapsed_seconds = time.monotonic() - state.start_monotonic
            # POST_PARSE delivery is synchronous today, so it otherwise inflates
            # invocation duration even though the proto explicitly excludes it.
            return int(max(elapsed_seconds - state.telemetry_delivery_seconds, 0.0) * 1000)

    def send_if_ready(self, invocation_id: str) -> None:
        # One caller drains ready phases. A concurrent caller returns while a send
        # is in flight; the drainer re-checks state after that attempt finishes.
        while True:
            with self._lock:
                state = self._states.get(invocation_id)
                if state is None or state.closed or state.sending:
                    return
                transport = state.transport
                # Authenticated endpoint only: no transport/headers means not ready.
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
        started = time.monotonic()
        try:
            body = encoder.encode_request(payload, event_id, workspace_id=workspace_id)
            client.send(host, body, header_factory=header_factory, workspace_id=workspace_id)
        except Exception as e:  # pragma: no cover - best-effort
            logger.debug(f"dbt telemetry: send failed (ignored): {e}")
        finally:
            delivery_seconds = time.monotonic() - started
            with self._lock:
                state = self._states.get(payload.invocation_id)
                if state is not None:
                    state.telemetry_delivery_seconds += delivery_seconds

    def close(self, invocation_id: str) -> None:
        with self._lock:
            # Leave a closed tombstone so a late callback cannot recreate state.
            state = self._states.get(invocation_id)
            if state is None:
                state = _InvocationState()
                self._states[invocation_id] = state
            state.closed = True
            # Keep only the tombstone; do not retain payload or auth state forever.
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


# Process-wide singleton.
_COORDINATOR = Coordinator()


def coordinator() -> Coordinator:
    return _COORDINATOR
