{% macro create_table_at(relation, intermediate_relation, compiled_code) %}
  {% set tags = config.get('databricks_tags') %}
  {%- set model_columns = model.get('columns', []) -%}
  {%- set existing_columns = adapter.get_columns_in_relation(intermediate_relation) -%}
  {% set model_constraints = model.get('constraints', []) %}
  {% set columns_and_constraints = adapter.parse_columns_and_constraints(existing_columns, model_columns, model_constraints) %}

  {%- set target_relation = relation.enrich(columns_and_constraints[1]) -%}
  {% call statement('main') %}
    {{ get_create_table_sql(target_relation, columns_and_constraints[0], intermediate_relation, compiled_code) }}
  {% endcall %}

  {{ apply_alter_constraints(target_relation) }}
  {{ apply_tags(target_relation, tags) }}

  {% call statement('merge into target') %}
    insert into {{ target_relation }} select * from {{ intermediate_relation }}
  {% endcall %}
{% endmacro %}

{% macro get_create_table_sql(target_relation, columns, intermediate_relation, compiled_code) %}
  {%- set file_format = config.get('file_format', default='delta') -%}
  {%- set contract = config.get('contract') -%}
  {%- set contract_enforced = contract and contract.enforced -%}
  {%- if contract_enforced -%}
    {{ get_assert_columns_equivalent(compiled_code) }}
  {% endif %}

  {% if file_format == 'delta' %}
    create or replace table {{ target_relation }}
  {% else %}
    create table {{ target_relation }}
  {% endif -%}
  {{ get_column_and_constraints_sql(target_relation, columns) }}
  {{ file_format_clause() }}
  {{ options_clause() }}
  {{ partition_cols(label="partitioned by") }}
  {{ liquid_clustered_cols() }}
  {{ clustered_cols(label="clustered by") }}
  {{ location_clause(target_relation) }}
  {{ comment_clause() }}
  {{ tblproperties_clause() }}
{% endmacro %}

{% macro databricks__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- else -%}
      {%- set file_format = config.get('file_format', default='delta') -%}
      {% if file_format == 'delta' %}
        create or replace table {{ relation.render() }}
      {% else %}
        create table {{ relation.render() }}
      {% endif %}
      {%- set contract_config = config.get('contract') -%}
      {% if contract_config and contract_config.enforced %}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
      {% endif %}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ liquid_clustered_cols() }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause(relation) }}
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

{% macro get_create_intermediate_table(relation, compiled_code, language) %}
  {%- if language == 'sql' -%}
    {{ create_temporary_view(relation, compiled_code) }}
  {%- else -%}
    {{ create_python_intermediate_table(relation, compiled_code) }}
  {%- endif -%}
{% endmacro %}