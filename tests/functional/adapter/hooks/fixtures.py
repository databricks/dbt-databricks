create_table_statement = """
create table {database}.`{schema}`.`on_model_hook` (
    test_state       STRING, -- start|end
    target_dbname    STRING,
    target_host      STRING,
    target_name      STRING,
    target_schema    STRING,
    target_type      STRING,
    target_user      STRING,
    target_pass      STRING,
    target_threads   INT,
    run_started_at   STRING,
    invocation_id    STRING,
    thread_id        STRING
)
"""

create_table_run_statement = """
create table {database}.`{schema}`.`on_run_hook` (
    test_state       STRING, -- start|end
    target_dbname    STRING,
    target_host      STRING,
    target_name      STRING,
    target_schema    STRING,
    target_type      STRING,
    target_user      STRING,
    target_pass      STRING,
    target_threads   INT,
    run_started_at   STRING,
    invocation_id    STRING,
    thread_id        STRING
)
"""

MODEL_PRE_HOOK = """
   insert into `{{this.database}}`.`{{this.schema}}`.`on_model_hook` (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'start',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into `{{this.database}}`.`{{this.schema}}`.`on_model_hook` (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'end',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""

hook_source_sql = """
create or replace table {database}.`{schema}`.hook_source using delta
as select 1 as id, 'one' as value
"""

hook_audit_sql = """
create or replace table {database}.`{schema}`.hook_audit
  (sequence int, phase string, relation_exists boolean) using delta
"""

record_hook_macros = """
{% macro record_hook(phase) %}
  {% if execute %}
    {% set query = 'show tables in ' ~ this.database ~ '.' ~ this.schema
       ~ " like '" ~ this.identifier ~ "'" %}
    {% set relation_exists = run_query(query).rows | length > 0 %}
  {% else %}
    {% set relation_exists = false %}
  {% endif %}
  insert into {{ target.database }}.{{ target.schema }}.hook_audit
  select coalesce(max(sequence), 0) + 1, '{{ phase }}', {{ relation_exists }}
  from {{ target.database }}.{{ target.schema }}.hook_audit
{% endmacro %}
"""

hook_model_sql = """
{% set materialization = var('hook_materialization', 'table') %}
{{ config(materialized=materialization, unique_key='id') }}
{% if materialization == 'metric_view' %}
version: 0.1
source: "{{ target.database }}.{{ target.schema }}.hook_source"
dimensions:
  - name: value
    expr: value
measures:
  - name: row_count
    expr: count(1)
{% elif materialization == 'streaming_table' %}
select * from stream {{ target.database }}.{{ target.schema }}.hook_source
{% else %}
select * from {{ target.database }}.{{ target.schema }}.hook_source
{% endif %}
"""

hook_helper_model_sql = """
{{ config(
    materialized='view',
    pre_hook=[
        before_begin("{{ record_hook('pre-outside') }}"),
        "{{ record_hook('pre-default') }}",
    ],
    post_hook=[
        "{{ record_hook('post-default') }}",
        after_commit("{{ record_hook('post-outside') }}"),
    ]
) }}
select * from {{ target.database }}.{{ target.schema }}.hook_source
"""

hook_seed_csv = "id,value\n1,one\n"

hook_snapshot_sql = """
{% snapshot hook_snapshot %}
{{ config(unique_key='id', strategy='check', check_cols=['value'], target_schema=target.schema) }}
select * from {{ target.database }}.{{ target.schema }}.hook_source
{% endsnapshot %}
"""

hooks_with_outside_category = {
    "+pre-hook": [
        "{{ record_hook('pre-default') }}",
        {"sql": "{{ record_hook('pre-outside') }}", "transaction": False},
    ],
    "+post-hook": [
        {"sql": "{{ record_hook('post-default') }}", "transaction": True},
        {"sql": "{{ record_hook('post-outside') }}", "transaction": False},
    ],
}
