{% macro tblproperties_clause() -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')()) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause() -%}
  {%- set tblproperties = config.get('tblproperties', {}) -%}
  {%- set is_iceberg = adapter.check_iceberg(config.get('table_format'), config.get('file_format'), model.config.materialized) -%}
  {%- if is_iceberg -%}
    {%- set _ = tblproperties.update({'delta.enableIcebergCompatV2': 'true', 'delta.universalFormat.enabledFormats': 'iceberg'}) -%}
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
  {% if tblproperties %}
    {%- set is_iceberg = adapter.check_iceberg(config.get('table_format'), config.get('file_format'), model.config.materialized) -%}
    {%- if is_iceberg -%}
      {%- set _ = tblproperties.update({'delta.enableIcebergCompatV2': 'true', 'delta.universalFormat.enabledFormats': 'iceberg'}) -%}
    {%- endif -%}
    {%- call statement('apply_tblproperties') -%}
      ALTER {{ relation.type }} {{ relation }} SET TBLPROPERTIES (
      {% for tblproperty in tblproperties -%}
        '{{ tblproperty }}' = '{{ tblproperties[tblproperty] }}' {%- if not loop.last %}, {% endif -%}
      {%- endfor %}
      )
    {%- endcall -%}
  {% endif %}
{%- endmacro -%}
