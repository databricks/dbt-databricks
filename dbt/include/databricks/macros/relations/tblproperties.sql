{% macro tblproperties_clause() -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')()) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause(tblproperties=None) -%}
  {%- if adapter.is_uniform(config) -%}
    {%- set tblproperties = adapter.update_tblproperties_for_uniform_iceberg(config, tblproperties) -%}
  {%- else -%}
    {%- set tblproperties = tblproperties or config.get("tblproperties", {}) -%}
  {%- endif -%}
  {%- if config.get("materialized") == "snapshot" -%}
    {%- set query_columns = none -%}
    {%- if execute -%}
      {%- set query_sql = none -%}
      {%- if model is defined and model -%}
        {%- if model is mapping -%}
          {%- set query_sql = model.get('compiled_code') -%}
        {%- elif model.compiled_code is defined and model.compiled_code is string -%}
          {%- set query_sql = model.compiled_code -%}
        {%- endif -%}
      {%- endif -%}
      {%- if query_sql -%}
        {%- set query_columns = get_columns_in_query(adapter.clean_sql(query_sql)) -%}
      {%- endif -%}
    {%- endif -%}
    {%- set tblproperties = adapter.update_tblproperties_for_snapshot(config, tblproperties, query_columns) -%}
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
