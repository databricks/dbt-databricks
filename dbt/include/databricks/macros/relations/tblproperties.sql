{% macro tblproperties_clause() -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')()) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause() -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- if tblproperties is not none %}
    tblproperties (
      {%- for prop in tblproperties -%}
      '{{ prop }}' = '{{ tblproperties[prop] }}' {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro apply_tblproperties_python(relation, tblproperties, language) -%}
  {%- if tblproperties and language == 'python' -%}
    {%- call statement('python_tblproperties') -%}
      alter table {{ relation }} set {{ tblproperties_clause() }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro -%}