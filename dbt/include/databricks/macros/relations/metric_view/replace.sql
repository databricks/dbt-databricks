{% macro databricks__get_replace_metric_view_sql(target_relation, sql) %}
  {{ get_create_metric_view_as_sql(target_relation, sql) }}
{% endmacro %}