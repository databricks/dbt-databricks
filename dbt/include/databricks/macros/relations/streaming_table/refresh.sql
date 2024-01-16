{% macro get_refresh_streaming_table_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_refresh_streaming_table_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__get_refresh_streaming_table_as_sql(relation, sql) -%}
  create or refresh streaming table {{ relation }}
  as
    {{ sql }}
{% endmacro %}