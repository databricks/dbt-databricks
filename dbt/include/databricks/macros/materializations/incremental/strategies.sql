{% macro databricks__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro databricks__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
    {{ return(get_insert_overwrite_sql(source, target)) }}
{% endmacro %}
