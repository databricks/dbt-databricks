{% macro get_create_sql_tblproperties(tblproperties) %}
  {%- set _ = adapter.check_iceberg(config.get('table_format'), config.get('file_format'), model.config.materialized) -%}
  {%- if tblproperties and tblproperties|length>0 -%}
    TBLPROPERTIES (
    {%- for prop in tblproperties -%}
      '{{ prop }}' = '{{ tblproperties[prop] }}'{%- if not loop.last -%}, {% endif -%}
    {% endfor -%}
    )
  {%- endif -%}
{% endmacro %}
