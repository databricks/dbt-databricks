{% materialization metric_view, adapter='databricks' -%}
  {%- set existing_relation = load_relation_with_metadata(this) -%}
  {%- set target_relation = this.incorporate(type='metric_view') -%}
  {% set grant_config = config.get('grants') %}
  {% set tags = config.get('databricks_tags') %}
  {% set sql = adapter.clean_sql(sql) %}

  {{ run_pre_hooks() }}

  {% if existing_relation %}
    {#- Metric views always use CREATE OR REPLACE - no alter path for now -#}
    {{ log('Using replace_with_metric_view (metric views always use replace)') }}
    {{ replace_with_metric_view(existing_relation, target_relation) }}
  {% else %}
    {% call statement('main') -%}
      {{ get_create_metric_view_as_sql(target_relation, sql) }}
    {%- endcall %}
    {{ apply_tags(target_relation, tags) }}
    {% set column_tags = adapter.get_column_tags_from_model(config.model) %}
    {% if column_tags and column_tags.set_column_tags %}
      {{ apply_column_tags(target_relation, column_tags) }}
    {% endif %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=True) %}

  {{ run_post_hooks() }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}