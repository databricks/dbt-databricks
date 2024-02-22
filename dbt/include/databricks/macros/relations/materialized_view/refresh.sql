{% macro get_refresh_materialized_view_sql(relation) -%}
  {{ adapter.dispatch('get_refresh_materialized_view_sql', 'dbt')(relation) }}
{%- endmacro %}

{% macro databricks__get_refresh_materialized_view_sql(relation) -%}
  refresh materialized view {{ relation }}
{% endmacro %}