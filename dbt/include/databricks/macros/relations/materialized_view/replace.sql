{% macro databricks__get_replace_materialized_view_sql(target_relation, sql) %}
  {{ get_create_materialized_view_as_sql(target_relation, sql) }}
{% endmacro %}

{#- A dropped view keeps its Compatibility Mode location reserved, so only a re-create with a location can fail with LOCATION_OVERLAP and leave no view; other rebuilds skip these metadata reads. -#}
{% macro get_mv_compatibility_rebuild_sql(existing_relation, target_relation, sql) %}
  {%- set location_key = 'delta.universalFormat.compatibility.location' -%}
  {%- set desired = adapter.get_config_from_model(config.model).config -%}
  {%- set desired_location = desired['tblproperties'].tblproperties.get(location_key) -%}
  {%- if desired_location is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set ns = namespace(existing_location=none, in_partitions=false) -%}
  {%- for row in fetch_tbl_properties(existing_relation).rows if row[0] == location_key -%}
    {%- set ns.existing_location = row[1] -%}
  {%- endfor -%}
  {%- set existing_partition_by = [] -%}
  {%- for row in describe_table_extended_without_caching(existing_relation).rows -%}
    {%- if row[0] == '# Partition Information' -%}
      {%- set ns.in_partitions = true -%}
    {%- elif ns.in_partitions and not row[0] -%}
      {%- set ns.in_partitions = false -%}
    {%- elif ns.in_partitions and not row[0].startswith('# ') -%}
      {%- do existing_partition_by.append(row[0]) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set decision = mv_compatibility_rebuild_strategy(
        desired_location, ns.existing_location, desired['partition_by'].partition_by, existing_partition_by
      ) -%}
  {%- if decision.strategy == 'replace' -%}
    {{ return(get_replace_materialized_view_sql(target_relation, sql)) }}
  {%- endif -%}
  {{ exceptions.raise_compiler_error("Cannot rebuild materialized view " ~ existing_relation ~ ": " ~ decision.reason) }}
{% endmacro %}
