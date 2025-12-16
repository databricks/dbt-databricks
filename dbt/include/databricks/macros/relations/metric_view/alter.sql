{% macro alter_metric_view(target_relation, changes) %}
  {{ log("Updating metric view via ALTER") }}
  {{ adapter.dispatch('alter_metric_view', 'dbt')(target_relation, changes) }}
{% endmacro %}

{% macro databricks__alter_metric_view(target_relation, changes) %}
  {% set tags = changes.get("tags") %}
  {% set tblproperties = changes.get("tblproperties") %}
  {% set query = changes.get("query") %}
  {% set column_comments = changes.get("column_comments") %}

  {# For metric views, only tags and tblproperties can be altered without recreating #}
  {% if tags %}
    {{ apply_tags(target_relation, tags.set_tags) }}
  {% endif %}
  {% if tblproperties %}
    {{ apply_tblproperties(target_relation, tblproperties.tblproperties) }}
  {% endif %}

  {# Query changes and column comment changes require full refresh for metric views #}
  {% if query or column_comments %}
    {{ exceptions.warn("Metric view YAML definition or column comment changes detected that cannot be applied via ALTER. These changes will require CREATE OR REPLACE on next run.") }}
  {% endif %}
{% endmacro %}

{% macro replace_with_metric_view(existing_relation, target_relation) %}
  {% set sql = adapter.clean_sql(sql) %}
  {% set tags = config.get('databricks_tags') %}
  {{ execute_multiple_statements(get_replace_sql(existing_relation, target_relation, sql)) }}
  {%- do apply_tags(target_relation, tags) -%}

  {% set column_tags = adapter.get_column_tags_from_model(config.model) %}
  {% if column_tags and column_tags.set_column_tags %}
    {{ apply_column_tags(target_relation, column_tags) }}
  {% endif %}
{% endmacro %}