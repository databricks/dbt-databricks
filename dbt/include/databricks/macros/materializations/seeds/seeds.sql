{% materialization seed, adapter='databricks' %}
  {% set target_relation = this.incorporate(type='table') %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and (old_relation.is_view or old_relation.is_materialized_view)) -%}
  {%- set exists_as_streaming_table = (old_relation is not none and old_relation.is_streaming_table) -%}

  {%- set grant_config = config.get('grants') -%}
  {%- set agate_table = load_agate_table() -%}

  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_pre_hooks() }}

  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view or a materialized view".format(old_relation)) }}
  {% elif exists_as_streaming_table %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a streaming table".format(old_relation)) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set sql = load_csv_rows(model, agate_table) %}

  {{ log_seed_operation(agate_table, full_refresh_mode, create_table_sql, sql) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  -- No need to persist docs, already handled in seed create

  {%- set tags = config.get('databricks_tags') -%}
  {%- if tags -%}
    {%- do apply_tags(target_relation, tags) -%}
  {%- endif -%}

  {% set column_tags = adapter.get_column_tags_from_model(config.model) %}
  {% if column_tags and column_tags.set_column_tags %}
    {{ apply_column_tags(target_relation, column_tags) }}
  {% endif %}

  {% do optimize(target_relation) %}

  {{ run_post_hooks() }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
