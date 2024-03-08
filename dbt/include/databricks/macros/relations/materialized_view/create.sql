{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  {%- set materialized_view = adapter.get_config_from_model(config.model) -%}
  {%- set partition_by = materialized_view.config["partition_by"].partition_by -%}
  {%- set tblproperties = materialized_view.config["tblproperties"].tblproperties -%}
  {%- set comment = materialized_view.config["comment"].comment -%}
  {%- set refresh = materialized_view.config["refresh"] -%}
  create materialized view {{ relation }}
  as
    {{ sql }}
{% endmacro %}