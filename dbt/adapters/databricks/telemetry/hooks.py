import sys
from typing import Any, Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry import builder, listener
from dbt.adapters.databricks.telemetry.config import (
    has_reusable_transport,
    is_enabled_for_invocation,
)
from dbt.adapters.databricks.telemetry.coordinator import Transport, coordinator


def _current_invocation_id() -> Optional[str]:
    try:
        from dbt_common.invocation import get_invocation_id

        invocation_id = get_invocation_id()
        return str(invocation_id) if invocation_id else None
    except Exception:
        return None


def on_adapter_init(adapter: Any) -> None:
    try:
        creds = getattr(getattr(adapter, "config", None), "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        coord = coordinator()
        coord.mark_start(invocation_id)
        if not listener.register():
            coord.close(invocation_id)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_adapter_init failed (ignored): {e}")


def on_post_parse(adapter: Any, manifest: Any) -> None:
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        log = builder.build_post_parse_log(
            manifest=manifest,
            config=config,
            creds=creds,
            behavior_flag=adapter.get_behavior_flag_no_warn,
        )
        if not log.invocation_id:
            return
        coord = coordinator()
        coord.record_ephemeral_ids(log.invocation_id, builder.ephemeral_resource_ids(manifest))
        coord.set_post_parse(log.invocation_id, log)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_post_parse failed (ignored): {e}")


def on_connection_open(
    credentials: Optional[DatabricksCredentials],
    credentials_manager: Optional[Any],
) -> None:
    try:
        if (
            not is_enabled_for_invocation(credentials)
            or not has_reusable_transport(credentials)
            or credentials_manager is None
        ):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        transport = Transport(
            host=getattr(credentials_manager, "host", None),
            header_factory=getattr(credentials_manager, "header_factory", None),
            workspace_id=getattr(credentials_manager, "workspace_id", None),
        )
        coordinator().set_transport(invocation_id, transport)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_connection_open failed (ignored): {e}")


def _finalize_post_run(invocation_id: str, exc_type: Optional[type]) -> None:
    coord = coordinator()
    results, selected, expected, coverage_complete, results_captured = coord.result_snapshot(
        invocation_id
    )
    task_success, fail_fast_triggered = coord.outcome_snapshot(invocation_id)
    log = builder.build_post_run_log(
        invocation_id,
        coord.elapsed_ms(invocation_id),
        exc_type,
        results,
        expected,
        coverage_complete,
        results_captured,
        selected_resources=selected,
        fail_fast_triggered=fail_fast_triggered,
        task_success=task_success,
    )
    coord.set_post_run(invocation_id, log)
    # Deliver the queued background sends before tearing down invocation state.
    coord.flush()
    coord.close(invocation_id)


def on_end_run_result(invocation_id: str) -> None:
    try:
        _finalize_post_run(invocation_id, None)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: EndRunResult finalization failed (ignored): {e}")


def on_run_end(adapter: Any) -> None:
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        # Normal runs finalize at EndRunResult, which fires after cleanup.
        exc_type = sys.exc_info()[0]
        if exc_type is not None:
            _finalize_post_run(invocation_id, exc_type)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_run_end failed (ignored): {e}")
