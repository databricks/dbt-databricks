
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
