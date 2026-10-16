{% macro databricks__get_replace_materialized_view_sql(target_relation, sql) %}
  {{ get_create_materialized_view_as_sql(target_relation, sql) }}
{% endmacro %}

{#- DROP + CREATE at a retained Compatibility Mode location fails with LOCATION_OVERLAP after the drop, leaving no view. -#}
{% macro get_mv_compatibility_rebuild_sql(existing_relation, target_relation, sql) %}
  {%- set model_config = adapter.get_config_from_model(config.model) -%}
  {%- set existing_config = adapter.get_relation_config(existing_relation, model_config) -%}
  {%- set decision = mv_compatibility_rebuild_strategy(
        model_config.config['partition_by'].partition_by,
        existing_config.config['partition_by'].partition_by,
        model_config.config['tblproperties'].tblproperties,
        existing_config.config['tblproperties'].tblproperties
      ) -%}
  {%- if decision is none -%}
    {{ return(none) }}
  {%- elif decision.strategy == 'replace' -%}
    {{ return(get_replace_materialized_view_sql(target_relation, sql)) }}
  {%- elif decision.strategy == 'drop' -%}
    {{ return(drop_and_create(existing_relation, target_relation, sql)) }}
  {%- endif -%}
  {{ exceptions.raise_compiler_error("Cannot rebuild materialized view " ~ existing_relation ~ ": " ~ decision.reason) }}
{% endmacro %}
