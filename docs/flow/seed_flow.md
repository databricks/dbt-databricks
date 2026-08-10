# Seed Flow

_Last updated: 2026-08-09_

> Two diagrams follow: **V1** is the default path, **V2** is used when the `use_materialization_v2`
> behavior flag is enabled. See [flow/README.md](README.md) for what the flag is and how the
> selection works. Source: `dbt/include/databricks/macros/materializations/seeds/seeds.sql`.

## V1 Seed Flow

```mermaid
flowchart LR
    AGATE[Create in memory table from CSV]
    STORE[Stores result of loading table]
    PRE["Run pre-hooks (inside_transaction=False)"]
    PRE2["Run pre-hooks (inside_transaction=True)"]
    RAISEV[Raise compiler error: view/MV target]
    RAISEST[Raise compiler error: streaming table target]
    COR[create or replace table...]
    CREATE[create table...]
    DROP[Drop existing table]
    INSERT[chunked inserts to table]
    GRANTS[Apply grants]
    INDEX["Create indexes"]
    POST["Run post-hooks (inside_transaction=True)"]
    POST2["Run post-hooks (inside_transaction=False)"]
    COMMIT["Commit transaction"]
    D1{Existing?}
    D2{Existing type?}
    D3{"Existing is replaceable and\ntarget format is Delta or Iceberg?"}
    D4{Full refresh or new?}
    AGATE-->STORE
    STORE-->PRE
    PRE-->PRE2-->D1
    D1--yes-->D2
    D1--"no"-->CREATE
    D2--"view/MV"-->RAISEV
    D2--"streaming table"-->RAISEST
    D2--table-->D3
    D3--yes-->COR
    COR-->INSERT
    D3--"no"-->DROP
    DROP-->CREATE
    CREATE-->INSERT
    INSERT-->GRANTS
    GRANTS-->D4
    D4--"yes"-->INDEX
    D4--"no"-->POST
    INDEX-->POST
    POST-->COMMIT
    COMMIT-->POST2
```

## V2 Seed Flow

V2 uses the shared `run_pre_hooks` and `run_post_hooks` helpers, which preserve the outside/inside
pre-hook and inside/outside post-hook ordering. Unlike V1, V2 has no explicit `COMMIT` and does not
call `create_indexes`. A view/materialized-view target and a streaming-table target raise distinct
compiler errors.

```mermaid
flowchart LR
    AGATE[Create in memory table from CSV]
    STORE[Stores result of loading table]
    PRE["Run pre-hooks (outside transaction)"]
    PRE2["Run pre-hooks (inside transaction)"]
    RAISEV[Raise compiler error: view/MV target]
    RAISEST[Raise compiler error: streaming table target]
    COR[create or replace table...]
    CREATE[create table...]
    DROP[Drop existing table]
    INSERT[chunked inserts to table]
    GRANTS[Apply grants]
    POST["Run post-hooks (inside transaction)"]
    POST2["Run post-hooks (outside transaction)"]
    D1{Existing?}
    D2{Existing type?}
    D3{"Existing is replaceable and\ntarget format is Delta or Iceberg?"}
    AGATE-->STORE
    STORE-->PRE
    PRE-->PRE2-->D1
    D1--yes-->D2
    D1--"no"-->CREATE
    D2--"view/MV"-->RAISEV
    D2--"streaming table"-->RAISEST
    D2--table-->D3
    D3--yes-->COR
    COR-->INSERT
    D3--"no"-->DROP
    DROP-->CREATE
    CREATE-->INSERT
    INSERT-->GRANTS
    GRANTS-->POST
    POST-->POST2
```
