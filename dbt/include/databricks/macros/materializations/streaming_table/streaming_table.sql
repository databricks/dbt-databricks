{% materialization streaming_table, adapter='databricks' %}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_streaming_table = (old_relation is not none and old_relation.is_streaming_table) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='streamingtable') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {%- set full_refresh_mode = should_full_refresh() -%}

  -- If there's a table with the same name and we were told to full refresh, drop it. 
  -- This behavior differs for Snowflake and BigQuery, so multiple dispatch is used.
  -- Also full refresh command is not yet supported for streaming tables so we drop/recreate.
  {%- if old_relation is not none and (not exists_as_streaming_table or full_refresh_mode) -%}
    {{ handle_existing_table(full_refresh_mode, old_relation) }}
    {%- set old_relation = None -%}
  {%- endif -%}
 
  -- create or refresh streaming table
  {%- if old_relation is not none -%}
    {% call statement('main') -%}
      {{ get_refresh_streaming_table_as_sql(target_relation, sql) }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
        {{ get_create_streaming_table_as_sql(target_relation, sql) }}
    {%- endcall %}
  {%- endif -%}
  
  {% set should_revoke = should_revoke(exists_as_streaming_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}