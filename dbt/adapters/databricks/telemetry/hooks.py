import sys
from typing import Any, Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.spog.extract import extract_workspace_id
from dbt.adapters.databricks.telemetry import builder, listener
from dbt.adapters.databricks.telemetry.config import (
    has_reusable_transport,
    is_enabled_for_invocation,
)
from dbt.adapters.databricks.telemetry.coordinator import Transport, coordinator

# Bound at adapter init. dbt-core resets the process-global invocation ID
# before leftover adapter cleanup, so teardown must not read the live ID.
_INVOCATION_ID_ATTR = "_dbt_telemetry_invocation_id"


def _current_invocation_id() -> Optional[str]:
    try:
        from dbt_common.invocation import get_invocation_id

        invocation_id = get_invocation_id()
        return str(invocation_id) if invocation_id else None
    except Exception:
        return None


def _stored_invocation_id(adapter: Any) -> Optional[str]:
    invocation_id = getattr(adapter, _INVOCATION_ID_ATTR, None)
    return str(invocation_id) if invocation_id else None


def on_adapter_init(adapter: Any) -> None:
    try:
        creds = getattr(getattr(adapter, "config", None), "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        if not has_reusable_transport(creds):
            logger.warning(
                "enable_dbt_telemetry is set but kernel OAuth U2M credentials cannot be "
                "reused for telemetry HTTP; events will not be sent."
            )
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        setattr(adapter, _INVOCATION_ID_ATTR, invocation_id)
        coord = coordinator()
        coord.mark_start(invocation_id)
        if not listener.register():
            coord.close(invocation_id)
    except Exception:  # pragma: no cover - best-effort
        return


def on_post_parse(adapter: Any, manifest: Any) -> None:
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        invocation_id = _current_invocation_id()
        coord = coordinator()
        if invocation_id and not coord.needs_post_parse(invocation_id):
            return
        log = builder.build_post_parse_log(
            manifest=manifest,
            config=config,
            creds=creds,
            behavior_flag=adapter.get_behavior_flag_no_warn,
        )
        if not log.invocation_id:
            return
        coord.record_ephemeral_ids(log.invocation_id, builder.ephemeral_resource_ids(manifest))
        coord.set_post_parse(log.invocation_id, log)
    except Exception:  # pragma: no cover - best-effort
        return


def on_connection_open(
    credentials: Optional[DatabricksCredentials],
    credentials_manager: Optional[Any],
    http_path: Optional[str] = None,
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
        workspace_id = extract_workspace_id(http_path)
        if workspace_id is None:
            workspace_id = getattr(credentials_manager, "workspace_id", None)
        transport = Transport(
            host=getattr(credentials_manager, "host", None),
            header_factory=getattr(credentials_manager, "header_factory", None),
            workspace_id=workspace_id,
        )
        coordinator().set_transport(invocation_id, transport)
    except Exception:  # pragma: no cover - best-effort
        return


def _finalize_post_run(
    invocation_id: str,
    exc_type: Optional[type],
    *,
    elapsed_ms: Optional[int] = None,
    command_success: Optional[bool] = None,
) -> None:
    coord = coordinator()
    if not coord.is_active(invocation_id):
        return
    results, selected, expected, coverage_complete, results_captured = coord.result_snapshot(
        invocation_id
    )
    task_success, fail_fast_triggered = coord.outcome_snapshot(invocation_id)
    if command_success is not None:
        task_success = command_success
    log = builder.build_post_run_log(
        invocation_id,
        coord.elapsed_ms(invocation_id) if elapsed_ms is None else elapsed_ms,
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
    coord.close(invocation_id)


def on_command_completed(invocation_id: str, success: Any, elapsed: Any) -> None:
    try:
        coord = coordinator()
        try:
            elapsed_ms = None
            if elapsed is not None:
                elapsed_ms = int(max(float(elapsed), 0.0) * 1000)
            command_success = None if success is None else bool(success)
            _finalize_post_run(
                invocation_id,
                None,
                elapsed_ms=elapsed_ms,
                command_success=command_success,
            )
        finally:
            if coord.is_active(invocation_id):
                coord.close(invocation_id)
            coord.flush()
    except Exception:  # pragma: no cover - best-effort
        return


def on_run_end(adapter: Any) -> None:
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled_for_invocation(creds):
            return
        invocation_id = _stored_invocation_id(adapter)
        if not invocation_id:
            return
        exc_type = sys.exc_info()[0]
        if exc_type is not None:
            _finalize_post_run(invocation_id, exc_type)
    except Exception:  # pragma: no cover - best-effort
        return
