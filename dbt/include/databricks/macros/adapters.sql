

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
