## The merge strategy

The merge incremental strategy requires:

- `file_format`: delta or hudi
- Databricks Runtime 5.1 and above for delta file format
- Apache Spark for hudi file format

dbt will run an [atomic `merge` statement](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html) which looks nearly identical to the default merge behavior on Snowflake and BigQuery.
If a `unique_key` is specified (recommended), dbt will update old records with values from new records that match on the key column.
If a `unique_key` is not specified, dbt will forgo match criteria and simply insert all new records (similar to `append` strategy).

Specifying `merge` as the incremental strategy is optional since it's the default strategy used when none is specified.

From v.1.9 onwards `merge` behavior can be tuned by setting the additional parameters.

- Merge steps control parameters that tweak the default behaviour:

  - `skip_matched_step`: if set to `true`, dbt will completely skip the `matched` clause of the merge statement.
  - `skip_not_matched_step`: similarly if `true` the `not matched` clause will be skipped.
  - `not_matched_by_source_action`: can be set to an action for the case the record does not exist in a source dataset. 
    - if set to `delete` the corresponding `when not matched by source ... then delete` clause will be added to the merge statement. 
    - if the action starts with `update` then the format `update set <actions>` is assumed, which will run update statement syntactically as provided.
      Can be multiline formatted.
    - in other cases by default no action is taken and now error raised.
  - `merge_with_schema_evolution`: when set to `true` dbt generates the merge statement with `WITH SCHEMA EVOLUTION` clause.

- Step conditions that are expressed with an explicit SQL predicates allow to execute corresponding action only in case the conditions are met in addition to matching by the `unique_key`.
  - `matched_condition`: applies to `when matched` step.
    In order to define such conditions one may use `DBT_INTERNAL_DEST` and `DBT_INTERNAL_SOURCE` as aliases for the target and source tables respectively, e.g. `DBT_INTERNAL_DEST.col1 = hash(DBT_INTERNAL_SOURCE.col2, DBT_INTERNAL_SOURCE.col3)`.
  - `not_matched_condition`: applies to `when not matched` step.
  - `not_matched_by_source_condition`: applies to `when not matched by source` step.
  - `target_alias`, `source_alias`: string values that will be used instead of `DBT_INTERNAL_DEST` and `DBT_INTERNAL_SOURCE` to distinguish between source and target tables in the merge statement.

Example below illustrates how these parameters affect the merge statement generation:

```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    target_alias='t',
    source_alias='s',
    matched_condition='t.tech_change_ts < s.tech_change_ts',
    not_matched_condition='s.attr1 IS NOT NULL',
    not_matched_by_source_condition='t.tech_change_ts < current_timestamp()',
    not_matched_by_source_action='''
        update set
            t.attr1 = 'deleted',
            t.tech_change_ts = current_timestamp()
    ''',
    merge_with_schema_evolution=true
) }}

select
    id,
    attr1,
    attr2,
    tech_change_ts
from
    {{ ref('source_table') }} as s
```

```sql
merge
    with schema evolution
into
    target_table as t
using (
    select
        id,
        attr1,
        attr2,
        tech_change_ts
    from
        source_table as s
)
on
    t.id <=> s.id
when matched
    and t.tech_change_ts < s.tech_change_ts
    then update set
        id = s.id,
        attr1 = s.attr1,
        attr2 = s.attr2,
        tech_change_ts = s.tech_change_ts

when not matched
    and s.attr1 IS NOT NULL
    then insert (
        id,
        attr1,
        attr2,
        tech_change_ts
    ) values (
        s.id,
        s.attr1,
        s.attr2,
        s.tech_change_ts
    )

when not matched by source
    and t.tech_change_ts < current_timestamp()
    then update set
        t.attr1 = 'deleted',
        t.tech_change_ts = current_timestamp()
```
