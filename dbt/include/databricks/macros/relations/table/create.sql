{% macro databricks__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- else -%}
      {% if config.get('file_format', default='delta') == 'delta' %}
        create or replace table {{ relation }}
      {% else %}
        create table {{ relation }}
      {% endif %}
      {%- set contract_config = config.get('contract') -%}
      {% if contract_config and contract_config.enforced %}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
      {% endif %}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ liquid_clustered_cols(label="cluster by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause() }}
      {{ comment_clause() }}
      {{ tblproperties_clause() }}
      as
      {{ compiled_code }}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}
    {{ databricks__py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
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

{% macro liquid_clustered_cols(label, required=false) -%}
  {{ return(adapter.dispatch('liquid_clustered_cols', 'dbt')(label, required)) }}
{%- endmacro -%}

{% macro databricks__liquid_clustered_cols(label, required=false) -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}