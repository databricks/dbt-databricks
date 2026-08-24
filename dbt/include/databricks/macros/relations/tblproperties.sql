{% macro tblproperties_clause() -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')()) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause(tblproperties=None, format_facts=None) -%}
  {%- if format_facts is not none -%}
    {%- set tblproperties = tblproperties or config.get("tblproperties", {}) -%}
    {%- if format_facts.table_format == 'iceberg' and format_facts.table_provider == 'delta' -%}
      {%- set tblproperties = adapter.update_tblproperties_for_uniform_iceberg(config, tblproperties) -%}
    {%- endif -%}
  {%- elif adapter.is_uniform(config) -%}
    {%- set tblproperties = adapter.update_tblproperties_for_uniform_iceberg(config, tblproperties) -%}
  {%- else -%}
    {%- set tblproperties = tblproperties or config.get("tblproperties", {}) -%}
  {%- endif -%}
  {%- if tblproperties != {} %}
    tblproperties (
      {%- for prop in tblproperties -%}
      '{{ prop }}' = '{{ tblproperties[prop] }}' {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro apply_tblproperties(relation, tblproperties) -%}
  {% set tblproperty_statment = databricks__tblproperties_clause(tblproperties) %}
  {% if tblproperty_statment %}
    {%- call statement('main') -%}
      ALTER {{ relation.type.render_for_alter() }} {{ relation.render() }} SET {{ tblproperty_statment}}
    {%- endcall -%}
  {% endif %}
{%- endmacro -%}
