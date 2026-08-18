"""Adapter lifecycle hooks feeding the coordinator. Both are opt-in gated and
defensive: any failure is swallowed so telemetry never affects a dbt command.
"""

import sys
from typing import Any, Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.telemetry import builder, listener
from dbt.adapters.databricks.telemetry.config import is_enabled
from dbt.adapters.databricks.telemetry.coordinator import Transport, coordinator


def _current_invocation_id() -> Optional[str]:
    try:
        from dbt_common.invocation import get_invocation_id

        invocation_id = get_invocation_id()
        return str(invocation_id) if invocation_id else None
    except Exception:
        return None


def on_adapter_init(adapter: Any) -> None:
    """Start the invocation timer and begin capturing per-node run results."""
    try:
        creds = getattr(getattr(adapter, "config", None), "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled(creds):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        coord = coordinator()
        coord.mark_start(invocation_id)
        coord.begin_result_capture(invocation_id)
        listener.register()
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_adapter_init failed (ignored): {e}")


def on_post_parse(adapter: Any, manifest: Any) -> None:
    """Build and register the POST_PARSE payload from the parsed manifest."""
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled(creds):
            return
        log = builder.build_post_parse_log(
            manifest=manifest,
            config=config,
            creds=creds,
            behavior_flag=adapter.get_behavior_flag_no_warn,
        )
        if not log.invocation_id:
            return
        coordinator().set_post_parse(log.invocation_id, log)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_post_parse failed (ignored): {e}")


def on_connection_open(
    credentials: Optional[DatabricksCredentials],
    credentials_manager: Optional[Any],
) -> None:
    """Register reusable transport from the first successful connection."""
    try:
        if not is_enabled(credentials) or credentials_manager is None:
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


def on_run_end(adapter: Any) -> None:
    """Build the POST_RUN payload at adapter teardown and close the invocation."""
    try:
        config = getattr(adapter, "config", None)
        creds = getattr(config, "credentials", None)
        if not isinstance(creds, DatabricksCredentials) or not is_enabled(creds):
            return
        invocation_id = _current_invocation_id()
        if not invocation_id:
            return
        coord = coordinator()
        node_results, results_captured = coord.result_snapshot(invocation_id)
        # sys.exc_info reflects any exception propagating through dbt's teardown.
        log = builder.build_post_run_log(
            invocation_id,
            coord.elapsed_ms(invocation_id),
            sys.exc_info()[0],
            node_results,
            results_captured,
        )
        coord.set_post_run(invocation_id, log)
        coord.close(invocation_id)
    except Exception as e:  # pragma: no cover - best-effort
        logger.debug(f"dbt telemetry: on_run_end failed (ignored): {e}")
