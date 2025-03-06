{% macro databricks__get_replace_view_sql(target_relation, sql) %}
  {{ create_view_as(relation, sql) }}
{% endmacro %}