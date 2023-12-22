{% macro get_create_sql_tblproperties(tblproperties) %}
  {%- if tblproperties and tblproperties|length>0 -%}
    TBLPROPERTIES (
    {%- for prop in tblproperties -%}
      '{{ prop }}' = '{{ tblproperties[prop] }}'{%- if not loop.last -%}, {% endif -%}
    {% endfor -%}
    )
  {%- endif -%}
{% endmacro %}