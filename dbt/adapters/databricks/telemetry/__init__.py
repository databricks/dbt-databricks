"""Opt-in dbt-databricks adapter telemetry. Emits a best-effort POST_PARSE
event (sanitized invocation, connection, project, and manifest-size aggregates)
to the authenticated workspace endpoint. Wiring lives in ``hooks``:
set_macro_resolver -> on_post_parse, first connection open -> on_connection_open.
"""

from dbt.adapters.databricks.telemetry.config import is_enabled

__all__ = ["is_enabled"]
