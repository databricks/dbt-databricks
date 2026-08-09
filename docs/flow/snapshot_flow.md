# Snapshot Flow

_Last updated: 2026-08-09_

> Snapshots do **not** use the `use_materialization_v2` flag — there is a single path. Source:
> `dbt/include/databricks/macros/materializations/snapshot.sql`. Strategy dispatch and the
> timestamp/check strategies are inherited from dbt Core; `snapshot_helpers.sql` contains the
> Databricks-specific merge, column, build, and staging helpers.

A snapshot always materializes to a `table`. The first run builds the table directly from the
snapshot query; subsequent runs stage the incoming rows, reconcile columns, and `MERGE` change
records into the existing snapshot table.

```mermaid
flowchart TD
    START[Resolve target relation] --> FMT{file_format in\ndelta / hudi?}
    FMT -- no --> RAISE1[Raise compiler error]
    FMT -- yes --> EXIST{Target relation\nexists?}

    EXIST -- "yes, wrong format" --> RAISE2[Raise compiler error]
    EXIST -- "yes, not a table" --> RAISE3[relation_wrong_type error]
    EXIST -- ok --> PRE[Run pre-hooks\n（outside then inside transaction）]

    PRE --> STRAT[Dispatch snapshot strategy\n（timestamp / check）]
    STRAT --> FIRST{Target relation\nexisted?}

    FIRST -- no --> BUILD[build_snapshot_table] --> CREATE[create_table_as target]
    FIRST -- yes --> ASSERT[Assert snapshot target\nvalid for strategy]
    ASSERT --> STAGE[Build snapshot staging table]
    STAGE --> EXPAND[expand_target_column_types]
    EXPAND --> ADDCOLS[create_columns for\nmissing columns]
    ADDCOLS --> MERGE[snapshot_merge_sql into target]

    CREATE --> TYPECHECK[check_time_data_types]
    MERGE --> TYPECHECK
    TYPECHECK --> MAIN["Execute main statement"]
    MAIN --> TAGS[Apply table tags + column tags]
    TAGS --> GRANTS[Apply grants]
    GRANTS --> DOCS[Persist docs]
    DOCS --> IDX{First run?}
    IDX -- yes --> CREATEIDX[create_indexes]
    IDX -- no --> POSTIN
    CREATEIDX --> POSTIN["Run post-hooks (inside transaction)"]
    POSTIN --> COMMIT[Commit]
    COMMIT --> CLEAN{Staging table\ncreated?}
    CLEAN -- yes --> POSTSNAP[post_snapshot cleanup]
    CLEAN -- no --> CONST
    POSTSNAP --> CONST[Persist constraints]
    CONST --> OPT[Run optimize]
    OPT --> POSTOUT["Run post-hooks (outside transaction)"]
```

Notes:

- **Format guard**: snapshots require `delta` or `hudi`; any other `file_format`, or an existing
  target in another format, raises a compiler error before any work is done.
- **First run vs. subsequent runs**: the branch on whether the target already existed is the
  central decision — a fresh `create_table_as` versus stage-and-`MERGE`.
- **Column reconciliation** (subsequent runs only): dbt drops its internal bookkeeping columns
  (`dbt_change_type`, `dbt_unique_key`, …) from the diff, expands types, and adds any genuinely
  missing columns to the target before merging.
