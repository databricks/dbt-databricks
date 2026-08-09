# View Flow

_Last updated: 2026-08-09_

> Two diagrams follow: **V1** is the default path, **V2** is used when the `use_materialization_v2`
> behavior flag is enabled. See [flow/README.md](README.md) for what the flag is and how the
> selection works. Source: `dbt/include/databricks/macros/materializations/view.sql`.

## V1 View Flow

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> WRONG{Existing relation is not a view?}
    WRONG -- yes --> HANDLE[handle_existing_table]
    WRONG -- no --> CREATE[Create or replace view]
    HANDLE --> CREATE
    CREATE --> GRANTS[Apply grants]
    GRANTS --> TAGS[Apply table tags]
    TAGS --> COLTAGS[Apply column tags]
    COLTAGS --> POST[Run post-hooks]
```

## V2 View Flow

Replacement branches use the shared [replace flow](replace_flow.md).

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> EXIST{Existing relation?}
    EXIST -- no --> CREATE[Create view]
    CREATE --> NEWTAGS[Apply table tags]
    NEWTAGS --> NEWCOLTAGS[Apply column tags]
    NEWCOLTAGS --> GRANTS[Apply grants]

    EXIST -- yes --> ALTERABLE{"Not full refresh, existing is view or metric view,\nand view_update_via_alter is true?"}
    ALTERABLE -- no --> REPLACE[replace_with_view]
    ALTERABLE -- yes --> HMS{Hive metastore?}
    HMS -- yes --> ERROR[Raise compiler error]
    HMS -- no --> CHANGES{Configuration changes?}
    CHANGES -- no --> NOOP[execute_no_op]
    CHANGES -- yes --> REFRESH{Changes require full refresh?}
    REFRESH -- yes --> REPLACE
    REFRESH -- no --> ALTER[alter_view]

    REPLACE --> TAGS[Apply table tags]
    TAGS --> COLTAGS[Apply column tags]
    COLTAGS --> GRANTS
    ALTER --> GRANTS
    NOOP --> GRANTS
    GRANTS --> POST[Run post-hooks]
```

`relation_should_be_altered` rejects the Hive metastore when alter-in-place was requested. A
replacement applies table and column tags after `get_replace_sql`; an in-place alter applies the
configuration changes returned by relation configuration comparison.
