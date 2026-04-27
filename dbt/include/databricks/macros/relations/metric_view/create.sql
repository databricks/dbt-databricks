{% macro get_create_metric_view_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_metric_view_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__get_create_metric_view_as_sql(relation, sql) %}
create or replace view {{ relation.render() }}
with metrics
language yaml
as $$
{{ sql }}
$$
{% endmacro %}