# Streaming Table Flow

_Last updated: 2026-08-09_

> Streaming tables do **not** use the `use_materialization_v2` flag — there is a single path.
> Source: `dbt/include/databricks/macros/materializations/streaming_table.sql`.
> [Materialized views](materialized_view_flow.md) follow the same shape.

The materialization computes a single `build_sql` describing the one scenario it is in (create /
full replace / refresh / alter / no-op), then either executes it or short-circuits to a no-op when
nothing needs to change.

```mermaid
flowchart TD
    START[Load existing relation] --> PRE["Run pre-hooks (outside transaction)"]
    PRE --> DECIDE{Determine scenario}

    DECIDE -- "no existing relation" --> CREATE[get_create_streaming_table_as_sql]
    DECIDE -- "full refresh OR\nexisting is not a\nstreaming table" --> REPLACE[get_replace_sql\n（see replace flow）]
    DECIDE -- "otherwise" --> CFG{Configuration\nchanges?}

    CFG -- "none" --> AUTO{refresh.auto_refreshed?}
    AUTO -- yes --> NOOPSQL[build_sql = ''\n（skip manual REFRESH）]
    AUTO -- no --> REFRESH[refresh_streaming_table]

    CFG -- "changes +\non_configuration_change=apply" --> ALTER[get_alter_streaming_table_as_sql]
    CFG -- "changes + continue" --> WARN[Warn; build_sql = '']
    CFG -- "changes + fail" --> FAIL[raise_fail_fast_error]

    CREATE --> CHECK
    REPLACE --> CHECK
    REFRESH --> CHECK
    ALTER --> CHECK
    NOOPSQL --> CHECK
    WARN --> CHECK
    CHECK{build_sql empty?}
    CHECK -- yes --> NOOP[execute_no_op\n（no server change）]
    CHECK -- no --> EXEC["execute_multiple_statements(build_sql)"]

    EXEC --> INTX["Run pre-hooks (inside transaction)"]
    INTX --> TAGS[Apply table tags]
    TAGS --> GRANTS[Apply grants]
    GRANTS --> COLTAGS[Apply column tags]
    COLTAGS --> POSTIN["Run post-hooks (inside transaction)"]
    NOOP --> POSTOUT
    POSTIN --> POSTOUT["Run post-hooks (outside transaction)"]
```

Notes:

- **Scenario selection** happens in `streaming_table_get_build_sql`, which returns the SQL string
  (possibly empty) for exactly one scenario.
- **No-op re-runs**: when there are no configuration changes and the streaming table is
  auto-refreshed, `build_sql` is empty and the run resolves to `execute_no_op` — no manual
  `REFRESH` is issued.
- **`on_configuration_change`** governs what happens when config drift is detected:
  `apply` alters in place, `continue` warns and skips, `fail` raises immediately.
- **Replace** (full refresh, or the existing relation is not a streaming table) delegates to the
  shared [replace flow](replace_flow.md).
