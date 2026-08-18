"""Run-event listener that captures per-node results for POST_RUN.

Added to dbt's event manager after setup_event_logger (which resets callbacks
in preflight), so it survives for the invocation. dbt fires NodeFinished once
per executed node with its resource_type and terminal status.
"""

from typing import Any

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry.coordinator import coordinator


def _current_invocation_id() -> str:
    from dbt_common.invocation import get_invocation_id

    return str(get_invocation_id() or "")


def _on_event(msg: Any) -> None:
    # Runs for every fired event; keep the non-NodeFinished path cheap.
    try:
        if msg.info.name != "NodeFinished":
            return
        node_info = msg.data.node_info
        invocation_id = _current_invocation_id()
        if invocation_id:
            coordinator().record_node_result(
                invocation_id, node_info.resource_type, node_info.node_status
            )
    except Exception:  # pragma: no cover - best-effort
        pass


def register() -> None:
    """Idempotently add the result listener to the event manager."""
    try:
        from dbt_common.events.event_manager_client import (
            add_callback_to_manager,
            get_event_manager,
        )

        if _on_event not in get_event_manager().callbacks:
            add_callback_to_manager(_on_event)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: listener register failed (ignored): {e}")
