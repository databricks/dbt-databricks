{% macro tblproperties_clause(catalog_relation=none) -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')(catalog_relation)) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause(catalog_relation=none, tblproperties=None) -%}
  {%- if catalog_relation is not none -%}
    {%- set model_tblproperties = config.get('tblproperties', {}) -%}
    {%- set all_tblproperties = {} -%}
    {%- do all_tblproperties.update(model_tblproperties) -%}
    
    {%- if catalog_relation.table_format == 'iceberg' -%}
      {%- do all_tblproperties.update(catalog_relation.iceberg_table_properties) -%}
    {%- endif -%}
    
    {%- if tblproperties is not none -%}
      {%- do all_tblproperties.update(tblproperties) -%}
    {%- endif -%}
    
    {%- set final_tblproperties = all_tblproperties -%}
  {%- else -%}
    {%- set final_tblproperties = adapter.update_tblproperties_for_iceberg(config, tblproperties) -%}
  {%- endif -%}

  {%- if final_tblproperties != {} %}
    tblproperties (
      {%- for prop in final_tblproperties -%}
      '{{ prop }}' = '{{ final_tblproperties[prop] }}' {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro apply_tblproperties(relation, tblproperties) -%}
  {% set tblproperty_statment = databricks__tblproperties_clause(none, tblproperties) %}
  {% if tblproperty_statment %}
    {%- call statement('main') -%}
      ALTER {{ relation.type }} {{ relation.render() }} SET {{ tblproperty_statment}}
    {%- endcall -%}
  {% endif %}
{%- endmacro -%}
