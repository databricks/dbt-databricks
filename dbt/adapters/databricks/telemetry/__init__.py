"""Opt-in dbt-databricks adapter telemetry. Wiring lives in ``hooks``:
set_macro_resolver -> on_post_parse, first connection open -> on_connection_open,
cleanup_connections -> on_run_end.
"""

from dbt.adapters.databricks.telemetry.config import is_enabled

__all__ = ["is_enabled"]
