{% macro make_staging_relation(base_relation, suffix='__dbt_stg') %}
  {% set stg_identifier = base_relation.identifier ~ suffix %}
  {% set stg_relation = api.Relation.create(database=base_relation.database, schema=base_relation.schema, identifier=stg_identifier, type='table') %}
  {% do return(stg_relation) %}
{% endmacro %}

{% macro databricks__make_intermediate_relation(base_relation, suffix) %}
    {{ return(databricks__make_temp_relation(base_relation, suffix)) }}
{% endmacro %}

{% macro databricks__make_temp_relation(base_relation, suffix='__dbt_tmp', as_table=False) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set language = model['language'] %}
    {%- if language == 'sql' -%}
      {% set tmp_relation = api.Relation.create(identifier=tmp_identifier, type='view') %}
    {%- else -%}
      {% set tmp_relation = api.Relation.create(database=base_relation.database, schema=base_relation.schema, identifier=tmp_identifier, type='table') %}
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

{% macro get_column_and_constraints_sql(relation, columns) %}
  (
    {% for column in columns %}
      {{ column.render_for_create() }}{% if not loop.last or relation.create_constraints %},{% endif %}
    {% endfor %}
    {% if relation.create_constraints %}
      {{ relation.render_constraints_for_create() }}
    {% endif %}
  )
{% endmacro %}