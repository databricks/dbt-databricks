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

{% macro apply_tblproperties(relation, tblproperties) -%}
  {% if tblproperties %}
    {%- call statement('apply_tblproperties') -%}
      ALTER {{ relation.type }} {{ relation }} SET TBLPROPERTIES (
      {% for tblproperty in tblproperties -%}
        '{{ tblproperty }}' = '{{ tblproperties[tblproperty] }}' {%- if not loop.last %}, {% endif -%}
      {%- endfor %}
      )
    {%- endcall -%}
  {% endif %}
{%- endmacro -%}
