"""Run-event listener that captures run results for POST_RUN.

Added to dbt's event manager after setup_event_logger (which resets callbacks
in preflight), so it survives for the invocation. It captures:
- EndRunResult: the authoritative final result set (includes fail-fast
  synthesized skips); does not fire on keyboard interrupt.
- NodeFinished: per-node results, the partial fallback used on interrupt.
- LogHookEndLine: auxiliary on-run-start/on-run-end results.
- ConcurrencyLine: node_count, the expected-result population.
- GenericExceptionOnRun/SkippingDetails: typed results that do not fire
  NodeFinished.
- RunResultFailure/RunResultError before EndRunResult: dbt-core's observable
  fail-fast catch path.
"""

from typing import Any

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry.coordinator import coordinator


def _current_invocation_id() -> str:
    from dbt_common.invocation import get_invocation_id

    return str(get_invocation_id() or "")


def _fail_fast_enabled() -> bool:
    try:
        from dbt.flags import get_flags

        return bool(getattr(get_flags(), "FAIL_FAST", False))
    except Exception:
        return False


def _on_event(msg: Any) -> None:
    # Runs for every fired event; keep the uninteresting path cheap.
    try:
        name = msg.info.name
        if name not in (
            "NodeFinished",
            "EndRunResult",
            "LogHookEndLine",
            "ConcurrencyLine",
            "GenericExceptionOnRun",
            "SkippingDetails",
            "RunResultFailure",
            "RunResultError",
        ):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        coord = coordinator()
        if name == "EndRunResult":
            # The authoritative status list includes fail-fast synthesized skips,
            # but RunResultMsg intentionally does not contain unique_id.
            coord.record_end_run(
                invocation_id,
                [r.status for r in msg.data.results],
                success=getattr(msg.data, "success", None),
            )
            # cleanup_connections ran immediately before this event, so this is
            # the normal/handled-run finalizer boundary.
            from dbt.adapters.databricks.telemetry import hooks

            hooks.on_end_run_result(invocation_id)
        elif name == "NodeFinished":
            # Partial per-node fallback used when EndRunResult never fires (interrupt).
            info = msg.data.node_info
            coord.record_node_result(invocation_id, info.unique_id, info.node_status)
        elif name == "LogHookEndLine":
            coord.record_hook_result(invocation_id, msg.data.status)
        elif name == "ConcurrencyLine":
            # node_count == selected non-ephemeral nodes == expected-result count.
            coord.record_expected_count(invocation_id, msg.data.node_count)
        elif name == "GenericExceptionOnRun":
            # dbt-core creates an error RunResult after this event but does not
            # subsequently fire NodeFinished for it.
            unique_id = msg.data.unique_id or msg.data.node_info.unique_id
            if unique_id:
                coord.record_node_result(invocation_id, unique_id, "error")
        elif name == "SkippingDetails":
            # Used when an on-run-start failure prevents selected nodes from
            # entering their normal runner/NodeFinished lifecycle.
            unique_id = msg.data.node_info.unique_id
            if unique_id:
                coord.record_node_result(invocation_id, unique_id, "skipped")
        elif name in ("RunResultFailure", "RunResultError") and _fail_fast_enabled():
            # In GraphRunnableTask, the caught FailFastError is printed before
            # EndRunResult. Normal end-of-run failure printing occurs afterward,
            # when the invocation has already been closed.
            coord.mark_fail_fast_triggered(invocation_id)
    except Exception:  # pragma: no cover - best-effort
        pass


def register() -> bool:
    """Idempotently add the result listener to the event manager."""
    try:
        from dbt_common.events.event_manager_client import (
            add_callback_to_manager,
            get_event_manager,
        )

        if _on_event not in get_event_manager().callbacks:
            add_callback_to_manager(_on_event)
        return True
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: listener register failed (ignored): {e}")
        return False
