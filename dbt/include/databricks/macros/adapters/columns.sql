
{% macro get_columns_comments(relation) -%}
  {% call statement('get_columns_comments', fetch_result=True) -%}
    describe table {{ relation|lower }}
  {% endcall %}

  {% do return(load_result('get_columns_comments').table) %}
{% endmacro %}

{% macro get_columns_comments_via_information_schema(relation) -%}
  {% call statement('repair_table', fetch_result=False) -%}
    REPAIR TABLE {{ relation|lower }} SYNC METADATA
  {% endcall %}
  {% call statement('get_columns_comments_via_information_schema', fetch_result=True) -%}
    select
      column_name,
      full_data_type,
      comment
    from `system`.`information_schema`.`columns`
    where
      table_catalog = '{{ relation.database|lower }}' and
      table_schema = '{{ relation.schema|lower }}' and 
      table_name = '{{ relation.identifier|lower }}'
  {% endcall %}

  {% do return(load_result('get_columns_comments_via_information_schema').table) %}
{% endmacro %}

{% macro databricks__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% if remove_columns %}
    {% if not relation.is_delta %}
      {{ exceptions.raise_compiler_error('Delta format required for dropping columns from tables') }}
    {% endif %}
    {%- call statement('alter_relation_remove_columns') -%}
      ALTER TABLE {{ relation.render() }} DROP COLUMNS ({{ api.Column.format_remove_column_list(remove_columns) }})
    {%- endcall -%}
  {% endif %}

  {% if add_columns %}
    {%- call statement('alter_relation_add_columns') -%}
      ALTER TABLE {{ relation.render() }} ADD COLUMNS ({{ api.Column.format_add_column_list(add_columns) }})
    {%- endcall -%}
  {% endif %}
{% endmacro %}

{% macro databricks__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
  {%- if select_sql_header is not none -%}
  {{ select_sql_header }}
  {%- endif %}
  select * from (
    {{ select_sql }}
  ) as __dbt_sbq
  where false
  limit 0
{% endmacro %}

{% macro databricks__get_columns_in_query(select_sql) %}
  {{ log("Getting column information via empty query")}}
  {% call statement('get_columns_in_query', fetch_result=True) -%}
     {{ get_empty_subquery_sql(select_sql) }}
  {% endcall %}
  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}