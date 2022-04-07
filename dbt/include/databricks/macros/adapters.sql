{% macro databricks__file_format_clause() %}
  {%- set file_format = config.get('file_format', default='delta') -%}
  {%- if file_format is not none %}
    using {{ file_format }}
  {%- endif %}
{%- endmacro -%}

{% macro databricks__options_clause() -%}
  {%- set options = config.get('options') -%}
  {%- if config.get('file_format', default='delta') == 'hudi' -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- if unique_key is not none and options is none -%}
      {%- set options = {'primaryKey': config.get('unique_key')} -%}
    {%- elif unique_key is not none and options is not none and 'primaryKey' not in options -%}
      {%- set _ = options.update({'primaryKey': config.get('unique_key')}) -%}
    {%- elif options is not none and 'primaryKey' in options and options['primaryKey'] != unique_key -%}
      {{ exceptions.raise_compiler_error("unique_key and options('primaryKey') should be the same column(s).") }}
    {%- endif %}
  {%- endif %}

  {%- if options is not none %}
    options (
      {%- for option in options -%}
      {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}


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


{% macro databricks__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ dbt_databricks_create_temporary_view(relation, sql) }}
  {%- else -%}
    {% if config.get('file_format', default='delta') == 'delta' %}
      create or replace table {{ relation }}
    {% else %}
      create table {{ relation }}
    {% endif %}
    {{ dbt_databricks_file_format_clause() }}
    {{ dbt_databricks_options_clause() }}
    {{ dbt_databricks_partition_cols(label="partitioned by") }}
    {{ dbt_databricks_clustered_cols(label="clustered by") }}
    {{ dbt_databricks_location_clause() }}
    {{ dbt_databricks_comment_clause() }}
    {{ dbt_databricks_tblproperties_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro databricks__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
  {{ dbt_databricks_comment_clause() }}
  {{ dbt_databricks_tblproperties_clause() }}
  as
    {{ sql }}
{% endmacro %}

{% macro databricks__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', default='delta') in ['delta', 'hudi'] %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation }} change column
            {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
            comment '{{ escaped_comment }}';
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
  {% endif %}
{% endmacro %}


{# backward compatitibily #}
{% macro dbt_databricks_file_format_clause() %}
  {{ return(file_format_clause()) }}
{%- endmacro -%}
{% macro dbt_databricks_location_clause() %}
  {{ return(location_clause()) }}
{%- endmacro -%}
{% macro dbt_databricks_options_clause() %}
  {{ return(options_clause()) }}
{%- endmacro -%}
{% macro dbt_databricks_comment_clause() %}
  {{ return(comment_clause()) }}
{%- endmacro -%}
{% macro dbt_databricks_partition_cols(label, required=false) %}
  {{ return(partition_cols(label, required)) }}
{%- endmacro -%}
{% macro dbt_databricks_clustered_cols(label, required=false) %}
  {{ return(clustered_cols(label, required)) }}
{%- endmacro -%}
{% macro dbt_databricks_tblproperties_clause() %}
  {{ return(tblproperties_clause()) }}
{%- endmacro -%}
{% macro dbt_databricks_create_temporary_view(relation, sql) -%}
  {{ return(create_temporary_view(relation, sql)) }}
{%- endmacro -%}
