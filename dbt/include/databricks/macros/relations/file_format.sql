{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', default='delta') -%}
  using {{ file_format }}
{%- endmacro -%}

{% macro get_file_format() %}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {% do return(dbt_databricks_validate_get_file_format(raw_file_format)) %}
{% endmacro %}