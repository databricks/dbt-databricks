{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  create materialized view {{ relation }}
  as
    {{ sql }}
{% endmacro %}