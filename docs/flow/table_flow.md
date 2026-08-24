# Table Flow

_Last updated: 2026-08-23_

> Two diagrams follow: **V1** is the default path, **V2** is used when the `use_materialization_v2`
> behavior flag is enabled. See [flow/README.md](README.md) for what the flag is and how the
> selection works. Source: `dbt/include/databricks/macros/materializations/table.sql`.

For SQL models on dbt-core versions that expose typed materialization execution, the adapter now
resolves the same flow into an ordered Python operation plan before mutation begins. Logical table
format, physical Delta versus managed-Iceberg provider, catalog type/provider, DBR capabilities,
and live replacement safety are plan inputs. The macros remain compatibility fallbacks and leaf SQL
renderers; they are not the source of those decisions on the typed path.

## V1 Table Flow

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> EXIST{Existing relation?}
    EXIST -- yes --> DROPNEEDED{"Shallow clone, non-table, or not a replaceable\nDelta/Iceberg table?"}
    EXIST -- no --> CREATE
    DROPNEEDED -- yes --> DROP[Drop existing relation]
    DROPNEEDED -- no --> CREATE
    DROP --> CREATE{Language?}
    CREATE -- SQL --> SQL[create table as / create or replace table as]
    CREATE -- Python --> PY[Create table with Python]
    SQL --> GRANTS[Apply grants]
    PY --> GRANTS
    GRANTS --> LANGUAGE{Python?}
    LANGUAGE -- yes --> PROPS[Apply tblproperties]
    LANGUAGE -- no --> TAGS[Apply table tags]
    PROPS --> TAGS
    TAGS --> COLTAGS[Apply column tags]
    COLTAGS --> DOCS[Persist docs]
    DOCS --> CONSTRAINTS[Persist constraints]
    CONSTRAINTS --> OPT[Run optimize]
    OPT --> POST[Run post-hooks]
```

V1 calls `run_hooks(pre_hooks)` without the outside/inside split used by seed and snapshot.

## V2 Table Flow

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> LANGUAGE{Language?}
    LANGUAGE -- SQL --> INTSQL[Create intermediate relation with SQL]
    LANGUAGE -- Python --> INTPY[Create intermediate relation with Python]
    INTSQL --> EXIST{Existing relation?}
    INTPY --> EXIST

    EXIST -- no --> CREATE["create_table_at target:<br/>create schema; constraints; table tags;<br/>column tags; insert intermediate"]
    EXIST -- yes --> SAFEPATH{"use_safer_relation_operations and\nexisting can be renamed?"}
    SAFEPATH -- yes --> SAFE["safe_relation_replace:<br/>create_table_at staging; back up existing;<br/>rename staging; drop backup; drop intermediate"]
    SAFEPATH -- no --> DROPNEEDED{"Shallow clone, non-table, or not a replaceable\nDelta/Iceberg table?"}
    DROPNEEDED -- yes --> DROP[Drop existing relation]
    DROPNEEDED -- no --> CREATE
    DROP --> CREATE

    CREATE --> GRANTS[Apply grants]
    SAFE --> GRANTS
    GRANTS --> OPT[Run optimize]
    OPT --> PYCLEAN{Python model?}
    PYCLEAN -- yes --> CLEAN[Drop intermediate relation]
    PYCLEAN -- no --> POST[Run post-hooks]
    CLEAN --> POST
```

The `create_table_at` helper applies constraints, table tags, and column tags before inserting from
the intermediate relation. The safe-replacement helper performs its own intermediate cleanup;
Python paths also clean up the intermediate relation after optimization. Unlike V1, V2 does not call
`persist_docs` — column and relation comments are handled on the create/insert path.
