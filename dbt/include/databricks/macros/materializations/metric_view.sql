{% materialization metric_view, adapter='databricks' -%}
  {%- set existing_relation = load_relation_with_metadata(this) -%}
  {%- set target_relation = this.incorporate(type='metric_view') -%}
  {% set grant_config = config.get('grants') %}
  {% set tags = config.get('databricks_tags') %}
  {% set sql = adapter.clean_sql(sql) %}

  {{ run_pre_hooks() }}

  {% if existing_relation %}
    {% if relation_should_be_altered(existing_relation) %}
      {% set configuration_changes = get_configuration_changes(existing_relation) %}
      {% if configuration_changes and configuration_changes.changes %}
        {% if configuration_changes.requires_full_refresh %}
          {{ log('Using replace_with_metric_view') }}
          {{ replace_with_metric_view(existing_relation, target_relation) }}
        {% else %}
          {{ log('Using alter_metric_view') }}
          {{ log(configuration_changes.changes) }}
          {{ alter_metric_view(target_relation, configuration_changes.changes) }}
        {% endif %}
      {% else %}
        {{ execute_no_op(target_relation) }}
      {% endif %}
    {% else %}
      {{ replace_with_metric_view(existing_relation, target_relation) }}
    {% endif %}
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