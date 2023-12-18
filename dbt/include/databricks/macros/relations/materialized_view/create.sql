{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}

  {%- set materialized_view = adapter.materialized_view_from_model(config.model) -%}
  
  create materialized view {{ relation }}
  {{ materialized_view.partition_by.to_sql_clause() }}
  {{ materialized_view.comment.to_sql_clause() }}
  {{ materialized_view.tblproperties.to_sql_clause() }}
  {{ materialized_view.refresh.to_sql_clause() }}
  as
    {{ sql }}
{% endmacro %}