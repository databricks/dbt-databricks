{% macro get_create_materialized_view_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_materialized_view_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  create or replace materialized view {{ relation }} snapshot
  as
    {{ sql }}
{% endmacro %}
