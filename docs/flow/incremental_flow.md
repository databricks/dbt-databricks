# Incremental Flow

_Last updated: 2026-08-19_

> Two diagrams follow: **Existing** is the default path, **New** is used when the
> `use_materialization_v2` behavior flag is enabled. See [flow/README.md](README.md) for what the
> flag is and how the selection works. Source:
> `dbt/include/databricks/macros/materializations/incremental/incremental.sql`.

## Existing Incremental Flow

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> EXIST{Existing relation?}
    EXIST -- no --> CREATE[Create target from model]
    CREATE --> NEWCFG["Apply constraints; table tags; column tags;<br/>Python tblproperties; persist docs"]

    EXIST -- yes --> REPLACE{"Existing is view, materialized view, or streaming table;\nor full refresh?"}
    REPLACE -- yes --> DROPNEEDED{"Not a replaceable Delta/Iceberg relation,\nor existing is a shallow clone?"}
    DROPNEEDED -- yes --> DROP[Drop existing relation]
    DROPNEEDED -- no --> RECREATE[Create or replace target from model]
    DROP --> RECREATE
    RECREATE --> WASVIEW{Existing was a view?}
    WASVIEW -- no --> REPLACECONST[Persist constraints]
    WASVIEW -- yes --> REPLACETAGS[Apply table tags]
    REPLACECONST --> REPLACETAGS
    REPLACETAGS --> REPLACECOLTAGS[Apply column tags]
    REPLACECOLTAGS --> REPLACEDOCS[Persist docs]

    REPLACE -- no --> DYNAMIC[Set dynamic overwrite mode when required]
    DYNAMIC --> DETECT[Detect configuration changes when enabled]
    DETECT --> TEMP[Create temporary relation from model]
    TEMP --> SCHEMA[Process schema changes]
    SCHEMA --> MERGE[Apply incremental strategy]
    MERGE --> CONFIG{"Configuration changes detected?"}
    CONFIG -- yes --> APPLYCFG["Apply in order: table tags; tblproperties;<br/>liquid clustering; row filter; column tags;<br/>constraints when contract-enforced and not HMS"]
    CONFIG -- no --> DOCS[Persist docs]
    APPLYCFG --> DOCS

    NEWCFG --> GRANTS[Apply grants]
    REPLACEDOCS --> GRANTS
    DOCS --> GRANTS
    GRANTS --> OPT[Run optimize]
    OPT --> POST[Run post-hooks]
    POST --> STATIC[Restore static overwrite mode for non-full-refresh insert_overwrite]
```

For an ordinary existing table, configuration changes are detected before the temporary relation
is built but are applied only after the incremental SQL runs. This ordering differs from V2.

## New Incremental Flow

```mermaid
flowchart LR
    PRE[Run pre-hooks] --> LANGUAGE{Language?}
    LANGUAGE -- SQL --> INTSQL[Create intermediate relation with SQL]
    LANGUAGE -- Python --> INTPY[Create intermediate relation with Python]
    INTSQL --> EXIST{Existing relation?}
    INTPY --> EXIST

    EXIST -- no --> CREATE["create_table_at target:<br/>create schema; constraints; table tags;<br/>column tags; insert intermediate"]
    EXIST -- yes --> SHOULDREPLACE{"Existing is DLT, a view,\nor full refresh?"}
    SHOULDREPLACE -- yes --> SAFEPATH{"use_safer_relation_operations and\nexisting can be renamed?"}
    SAFEPATH -- yes --> SAFE["safe_relation_replace:<br/>create_table_at staging; back up existing;<br/>rename staging; drop backup; drop intermediate"]
    SAFEPATH -- no --> DROPNEEDED{"Existing is not replaceable Delta/Iceberg,\nor is a shallow clone?"}
    DROPNEEDED -- yes --> DROP[Drop existing relation]
    DROPNEEDED -- no --> CREATE
    DROP --> CREATE

    SHOULDREPLACE -- no --> DYNAMIC[Set dynamic overwrite mode when required]
    DYNAMIC --> SCHEMA[Process schema changes]
    SCHEMA --> CONFIG["When incremental_apply_config_changes is enabled,<br/>process before merge: table tags; tblproperties;<br/>liquid clustering; relation comment; column comments;<br/>column tags; constraints; column masks; row filter"]
    CONFIG --> MERGE[Apply incremental strategy]

    CREATE --> GRANTS[Apply grants]
    SAFE --> GRANTS
    MERGE --> GRANTS
    GRANTS --> OPT[Run optimize]
    OPT --> PYCLEAN{Python model?}
    PYCLEAN -- yes --> CLEAN[Drop intermediate relation]
    PYCLEAN -- no --> POST[Run post-hooks]
    CLEAN --> POST
    POST --> STATIC[Restore static overwrite mode for non-full-refresh insert_overwrite]
```

V2 replaces only DLT relations, views, and full-refresh targets. An ordinary existing table takes
the incremental branch even when its configuration changes. Safe staging is selected only when
`use_safer_relation_operations` is enabled and the existing relation can be renamed; otherwise a
non-replaceable relation or shallow clone is dropped before `create_table_at`. Unlike the Existing
path, V2 does not call `persist_docs` — relation and column comments are handled via
`apply_config_changeset` or the create/insert path. On the fallback metadata path, the three
constraint-specific `information_schema` queries run only when the model has an enforced contract;
the consolidated `DESCRIBE TABLE EXTENDED AS JSON` path is unchanged.
