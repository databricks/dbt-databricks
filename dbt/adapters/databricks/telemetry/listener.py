from typing import Any

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
    try:
        name = msg.info.name
        if name not in (
            "NodeFinished",
            "EndRunResult",
            "CommandCompleted",
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
            if not coord.is_active(invocation_id):
                return
            coord.record_end_run(
                invocation_id,
                [r.status for r in msg.data.results],
                success=getattr(msg.data, "success", None),
            )
        elif name == "CommandCompleted":
            from dbt.adapters.databricks.telemetry import hooks

            hooks.on_command_completed(
                invocation_id,
                getattr(msg.data, "success", None),
                getattr(msg.data, "elapsed", None),
            )
        elif name == "NodeFinished":
            info = msg.data.node_info
            coord.record_node_result(invocation_id, info.unique_id, info.node_status)
        elif name == "LogHookEndLine":
            coord.record_hook_result(invocation_id, msg.data.status)
        elif name == "ConcurrencyLine":
            coord.record_expected_count(invocation_id, msg.data.node_count)
        elif name == "GenericExceptionOnRun":
            # No NodeFinished event follows.
            unique_id = msg.data.unique_id or msg.data.node_info.unique_id
            if unique_id:
                coord.record_node_result(invocation_id, unique_id, "error")
        elif name == "SkippingDetails":
            unique_id = msg.data.node_info.unique_id
            if unique_id:
                coord.record_node_result(invocation_id, unique_id, "skipped")
        elif name in ("RunResultFailure", "RunResultError") and _fail_fast_enabled():
            coord.mark_fail_fast_triggered(invocation_id)
    except Exception:  # pragma: no cover - best-effort
        return


def register() -> bool:
    try:
        from dbt_common.events.event_manager_client import (
            add_callback_to_manager,
            get_event_manager,
        )

        if _on_event not in get_event_manager().callbacks:
            add_callback_to_manager(_on_event)
        return True
    except Exception:  # pragma: no cover - best-effort
        return False
