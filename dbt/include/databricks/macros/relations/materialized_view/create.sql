{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  {# Column masks are supported in DBSQL, but not yet wired up to the adapter. Return a helpful error until supported. #}
  {% if column_mask_exists() %}
    {% do exceptions.raise_compiler_error("Column masks are not yet supported for materialized views.") %}
  {% endif %}
  {%- set materialized_view = adapter.get_config_from_model(config.model) -%}
  {%- set partition_by = materialized_view.config["partition_by"].partition_by -%}
  {%- set tblproperties = materialized_view.config["tblproperties"].tblproperties -%}
  {%- set comment = materialized_view.config["comment"].comment -%}
  {%- set refresh = materialized_view.config["refresh"] -%}

  {%- set columns = get_column_schema_from_query(sql) -%}
  {%- set model_columns = model.get('columns', {}) -%}
  {%- set model_constraints = model.get('constraints', []) -%}
  {%- set columns_and_constraints = adapter.parse_columns_and_constraints(columns, model_columns, model_constraints) -%}
  {%- set target_relation = relation.enrich(columns_and_constraints[1]) -%}

  create materialized view {{ target_relation.render() }}
    {{ get_column_and_constraints_sql(target_relation, columns_and_constraints[0]) }}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
  as
    {{ sql }}
{% endmacro %}
