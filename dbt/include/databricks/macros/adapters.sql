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
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
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
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}

{% macro databricks__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
  {{ comment_clause() }}
  {{ tblproperties_clause() }}
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

{# Persist table-level and column-level constraints. #}
{% macro persist_constraints(relation, model) %}
  {{ return(adapter.dispatch('persist_constraints', 'dbt')(relation, model)) }}
{% endmacro %}

{% macro databricks__persist_constraints(relation, model) %}
  {% if config.get('persist_constraints', False) and config.get('file_format', 'delta') == 'delta' %}
    {% do alter_table_add_constraints(relation, model.meta.constraints) %}
    {% do alter_column_set_constraints(relation, model.columns) %}
  {% endif %}
{% endmacro %}

{% macro alter_table_add_constraints(relation, constraints) %}
  {{ return(adapter.dispatch('alter_table_add_constraints', 'dbt')(relation, constraints)) }}
{% endmacro %}

{% macro databricks__alter_table_add_constraints(relation, constraints) %}
  {% if constraints is sequence %}
    {% for constraint in constraints %}
      {% set name = constraint['name'] %}
      {% if not name %}
        {{ exceptions.raise_compiler_error('Invalid check constraint name: ' ~ name) }}
      {% endif %}
      {% set condition = constraint['condition'] %}
      {% if not condition %}
        {{ exceptions.raise_compiler_error('Invalid check constraint condition: ' ~ condition) }}
      {% endif %}
      {# Skip if the update is incremental. #}
      {% if not is_incremental() %}
        {% call statement() %}
          alter table {{ relation }} add constraint {{ name }} check ({{ condition }});
        {% endcall %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro alter_column_set_constraints(relation, column_dict) %}
  {{ return(adapter.dispatch('alter_column_set_constraints', 'dbt')(relation, column_dict)) }}
{% endmacro %}

{% macro databricks__alter_column_set_constraints(relation, column_dict) %}
  {% for column_name in column_dict %}
    {% set constraint = column_dict[column_name]['meta']['constraint'] %}
    {% if constraint %}
      {% if constraint != 'not_null' %}
        {{ exceptions.raise_compiler_error('Invalid constraint for column ' ~ column_name ~ '. Only `not_null` is supported.') }}
      {% endif %}
      {% set quoted_name = adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name %}
      {% call statement() %}
        alter table {{ relation }} change column {{ quoted_name }} set not null
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro databricks__list_relations_without_caching(schema_relation) %}
  {{ return(adapter.get_relations_without_caching(schema_relation)) }}
{% endmacro %}

{% macro show_table_extended(schema_relation) %}
  {{ return(adapter.dispatch('show_table_extended', 'dbt')(schema_relation)) }}
{% endmacro %}

{% macro databricks__show_table_extended(schema_relation) %}
  {% call statement('show_table_extended', fetch_result=True) -%}
    show table extended in {{ schema_relation.without_identifier() }} like '{{ schema_relation.identifier }}'
  {% endcall %}

  {% do return(load_result('show_table_extended').table) %}
{% endmacro %}

{% macro show_tables(relation) %}
  {{ return(adapter.dispatch('show_tables', 'dbt')(relation)) }}
{% endmacro %}

{% macro databricks__show_tables(relation) %}
  {% call statement('show_tables', fetch_result=True) -%}
    show tables in {{ relation }}
  {% endcall %}

  {% do return(load_result('show_tables').table) %}
{% endmacro %}

{% macro show_views(relation) %}
  {{ return(adapter.dispatch('show_views', 'dbt')(relation)) }}
{% endmacro %}

{% macro databricks__show_views(relation) %}
  {% call statement('show_views', fetch_result=True) -%}
    show views in {{ relation }}
  {% endcall %}

  {% do return(load_result('show_views').table) %}
{% endmacro %}

{% macro databricks__generate_database_name(custom_database_name=none, node=none) -%}
    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}
        {{ return(default_database) }}
    {%- else -%}
        {{ return(custom_database_name) }}
    {%- endif -%}
{%- endmacro %}

{% macro databricks__make_temp_relation(base_relation, suffix='__dbt_tmp', as_table=False) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {%- if as_table -%}
        {% set tmp_relation = api.Relation.create(
            identifier=tmp_identifier,
            schema=base_relation.schema,
            database=base_relation.database,
            type='table') %}
    {%- else -%}
        {% set tmp_relation = api.Relation.create(identifier=tmp_identifier, type='view') %}
    {%- endif -%}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro databricks__get_or_create_relation(database, schema, identifier, type, needs_information=False) %}
  {%- set target_relation = adapter.get_relation(
            database=database,
            schema=schema,
            identifier=identifier,
            needs_information=needs_information) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}
