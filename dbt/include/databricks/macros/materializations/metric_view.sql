{% materialization metric_view, adapter='databricks' -%}
  {%- set existing_relation = load_relation_with_metadata(this) -%}
  {%- set target_relation = this.incorporate(type='metric_view') -%}
  {% set grant_config = config.get('grants') %}
  {% set tags = config.get('databricks_tags') %}
  {% set sql = adapter.clean_sql(sql) %}

  {{ run_pre_hooks() }}

  {% if existing_relation %}
    {#- Only use alter path if existing relation is actually a metric_view -#}
    {% if existing_relation.is_metric_view and relation_should_be_altered(existing_relation) %}
      {% set configuration_changes = get_configuration_changes(existing_relation) %}
      {% if configuration_changes and configuration_changes.changes %}
        {% if configuration_changes.requires_full_refresh %}
          {{ replace_with_metric_view(existing_relation, target_relation) }}
        {% else %}
          {{ alter_metric_view(target_relation, configuration_changes.changes) }}
        {% endif %}
      {% else %}
        {# No changes detected - run a no-op statement for dbt tracking #}
        {% call statement('main') %}
          select 1
        {% endcall %}
      {% endif %}
    {% else %}
      {{ replace_with_metric_view(existing_relation, target_relation) }}
    {% endif %}
  {% else %}
    {% call statement('main') -%}
      {{ get_create_metric_view_as_sql(target_relation, sql) }}
    {%- endcall %}
    {{ apply_tags(target_relation, tags) }}
  {% endif %}

  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke(existing_relation, full_refresh_mode=True)) %}

  {{ run_post_hooks() }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
