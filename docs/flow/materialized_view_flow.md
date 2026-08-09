# Materialized View Flow

_Last updated: 2026-08-09_

> Materialized views do **not** use the `use_materialization_v2` flag — there is a single path.
> Source: `dbt/include/databricks/macros/materializations/materialized_view.sql`.

The materialized view materialization is structurally identical to the
[streaming table flow](streaming_table_flow.md): it computes a single `build_sql` for the one
scenario it is in (create / full replace / refresh / alter / no-op), then executes it or
short-circuits to a no-op. Only the underlying SQL helpers differ
(`get_create_materialized_view_as_sql`, `refresh_materialized_view`,
`get_alter_materialized_view_as_sql`).

```mermaid
flowchart TD
    START[Load existing relation] --> PRE["Run pre-hooks (outside transaction)"]
    PRE --> DECIDE{Determine scenario}

    DECIDE -- "no existing relation" --> CREATE[get_create_materialized_view_as_sql]
    DECIDE -- "full refresh OR\nexisting is not a\nmaterialized view" --> REPLACE[get_replace_sql\n（see replace flow）]
    DECIDE -- "otherwise" --> CFG{Configuration\nchanges?}

    CFG -- "none" --> AUTO{refresh.auto_refreshed?}
    AUTO -- yes --> NOOPSQL[build_sql = ''\n（skip manual REFRESH）]
    AUTO -- no --> REFRESH[refresh_materialized_view]

    CFG -- "changes +\non_configuration_change=apply" --> ALTER[get_alter_materialized_view_as_sql]
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
    TAGS --> COLTAGS[Apply column tags]
    COLTAGS --> GRANTS[Apply grants]
    GRANTS --> POSTIN["Run post-hooks (inside transaction)"]
    NOOP --> POSTOUT
    POSTIN --> POSTOUT["Run post-hooks (outside transaction)"]
```

Notes:

- See the [streaming table flow](streaming_table_flow.md) notes — the scenario selection, no-op
  handling, `on_configuration_change` semantics, and delegation to the shared
  [replace flow](replace_flow.md) are the same.
