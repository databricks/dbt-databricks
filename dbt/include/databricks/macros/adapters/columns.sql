
{% macro get_columns_comments(relation) -%}
  {{ return(run_query(get_columns_comments_sql(relation))) }}
{% endmacro %}

{% macro get_columns_comments_sql(relation) %}
DESCRIBE TABLE {{ relation.render() }}
{% endmacro %}

{% macro get_columns_comments_via_information_schema(relation) -%}
  {% call statement('repair_table', fetch_result=False) -%}
    {{ repair_table_sql(relation) }}
  {% endcall %}
  {{ return(run_query(get_columns_comments_via_information_schema_sql(relation))) }}
{% endmacro %}

{% macro repair_table_sql(relation) %}
REPAIR TABLE {{ relation.render() }} SYNC METADATA
{% endmacro %}

{% macro get_columns_comments_via_information_schema_sql(relation) %}
SELECT
  column_name,
  full_data_type,
  comment
FROM `system`.`information_schema`.`columns`
WHERE
  table_catalog = '{{ relation.database|lower }}' and
  table_schema = '{{ relation.schema|lower }}' and 
  table_name = '{{ relation.identifier|lower }}'
{% endmacro %}

{% macro databricks__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% if remove_columns %}
    {% if not relation.is_delta %}
      {{ exceptions.raise_compiler_error('Delta format required for dropping columns from tables') }}
    {% endif %}
    {%- call statement('alter_relation_remove_columns') -%}
      {{ drop_columns_sql(relation, remove_columns) }}
    {%- endcall -%}
  {% endif %}

  {% if add_columns %}
    {% if not relation.is_delta %}
      {{ exceptions.raise_compiler_error('Delta format required for dropping columns from tables') }}
    {% endif %}
    {%- call statement('alter_relation_add_columns') -%}
      {{ add_columns_sql(relation, add_columns) }}
    {%- endcall -%}
  {% endif %}
{% endmacro %}

{% macro drop_columns_sql(relation, remove_columns) %}
ALTER TABLE {{ relation.render() }} DROP COLUMNS ({{ api.Column.format_remove_column_list(remove_columns) }})
{% endmacro %}

{% macro add_columns_sql(relation, add_columns) %}
ALTER TABLE {{ relation.render() }} ADD COLUMNS ({{ api.Column.format_add_column_list(add_columns) }})
{% endmacro %}
