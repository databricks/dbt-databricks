{% materialization materialized_view, adapter='databricks' %}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_materialized_view = (old_relation is not none and old_relation.is_materialized_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='materializedview') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {%- set full_refresh_mode = should_full_refresh() -%}

  {%- if exists_as_materialized_view and not full_refresh_mode -%}
    -- refresh materialized view
    {% call statement('main') -%}
      {{ get_refresh_materialized_view_sql(target_relation) }}
    {%- endcall %}
  {%- else -%}
    -- If there's a table with the same name and we weren't told to full refresh,
    -- that's an error. If we were told to full refresh, drop it. This behavior differs
    -- for Snowflake and BigQuery, so multiple dispatch is used.
    {%- if old_relation is not none and (not exists_as_materialized_view or full_refresh_mode) -%}
      {{ handle_existing_table(full_refresh_mode, old_relation) }}
    {%- endif -%}

    -- build model
    {% call statement('main') -%}
      {{ get_create_materialized_view_as_sql(target_relation, sql) }}
    {%- endcall %}
  {%- endif -%}

  {% set should_revoke = should_revoke(exists_as_materialized_view, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}