{#-- CREATE embeds an explicit column list (Databricks user-specified schema).
    When that drifts from the current query (e.g. upstream `select *` column add),
    REFRESH fails — recreate instead (#1359). Streaming tables (#1303) are out of
    scope here: locking columns on CREATE is a separate design question. --#}
{% macro dlt_inferred_query_schema_changed(relation, sql) -%}
  {%- set temp_relation = make_temp_relation(relation) -%}
  {%- set view_sql = sql.rstrip('; \n\t') ~ ' LIMIT 10' -%}
  {% call statement('dlt_schema_drift_temp_view') -%}
    {{ create_temporary_view(temp_relation, view_sql) }}
  {%- endcall %}
  {%- set inferred_columns = adapter.get_columns_in_relation(temp_relation) -%}
  {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
  {%- do return(adapter.column_schemas_differ(existing_columns, inferred_columns)) -%}
{%- endmacro %}
