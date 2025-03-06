---
Replace Flow
---

# Replace Flow

Diagram of the replace decision tree.

```mermaid
flowchart TD
    SAFE{{use_safer_relation_operations?}}
    D1{Existing is replaceable?}
    D2{Is view?}
    D3{Is table?}
    D4{Target can be renamed?}
    D5{Existing can be renamed?}
    D6{Existing can be renamed?}

    SAFE_REPLACE["Safely replace -
        get_create_intermediate_sql(target_relation, sql),
        get_create_backup_sql(existing_relation),
        get_rename_intermediate_sql(target_relation),
        get_drop_backup_sql(existing_relation)"]

    STAGE_THEN_REPLACE["Stage then replace -
        get_create_intermediate_sql(target_relation, sql),
        get_drop_sql(existing_relation),
        get_rename_intermediate_sql(target_relation)"]

    BACKUP_THEN_REPLACE["Backup then write to target -
        create_backup(existing_relation)
        get_create_sql(target_relation, sql),
        get_drop_backup_sql(existing_relation)"]

    DROP_AND_CREATE["Drop and create -
        get_drop_sql(existing_relation),
        get_create_sql(target_relation, sql)"]

    SAFE--False-->D1
    SAFE--True-->D4
    D1--True-->D2
    D2--True-->get_replace_view_sql
    D2--False-->D3
    D3--True-->get_replace_table_sql
    D3--False-->D4
    D4--True-->D5
    D4--False-->D6
    D5--True-->SAFE_REPLACE
    D5--False-->STAGE_THEN_REPLACE
    D6--True-->BACKUP_THEN_REPLACE
    D6--False-->DROP_AND_CREATE
```

| Existing type    | Target type      | Safe Replace? | Outcome              |
| ---------------- | ---------------- | ------------- | -------------------- |
| VIEW             | VIEW             | True          | Safely replace       |
| VIEW(Delta)      | VIEW(Delta)      | False         | CREATE OR REPLACE... |
| VIEW(Non-delta)  | VIEW(Delta)      | Either        | Safely replace       |
| VIEW(Delta)      | VIEW(Non-delta)  | Either        | Safely replace       |
| VIEW             | TABLE            | Either        | Safely replace       |
| VIEW             | MV/ST            | Either        | Backup then replace  |
| TABLE            | TABLE            | True          | Safely replace       |
| TABLE(Delta)     | TABLE(Delta)     | False         | CREATE OR REPLACE... |
| TABLE(Non-delta) | TABLE(Delta)     | Either        | Safely replace       |
| TABLE(Delta)     | TABLE(Non-delta) | Either        | Safely replace       |
| TABLE            | VIEW             | Either        | Safely replace       |
| TABLE            | MV/ST            | Either        | Backup then replace  |
| MV/ST            | MV/ST            | Either        | Drop and create      |
| MV/ST            | VIEW             | Either        | Stage then replace   |
| MV/ST            | TABLE            | Either        | Stage then replace   |
