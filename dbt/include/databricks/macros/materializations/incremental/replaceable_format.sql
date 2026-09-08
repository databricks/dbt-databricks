{#-- True when `create or replace table` can stand in for drop-then-create on a full refresh.

     Managed Iceberg needs its own arm rather than reusing `file_format`: an Iceberg model keeps
     `file_format` at delta (`iceberg_table_properties` raises for anything else) and `iceberg` is
     not an accepted `file_format` at all, so the `file_format == 'iceberg'` test this replaced
     could never be true. The target is Iceberg exactly when `table_format` is iceberg and the
     behavior flag is on -- the same condition `file_format_clause` uses to emit `using iceberg`.
     The flag is read without warning because this runs for every incremental model, including
     projects that never opt in (issue #1266). --#}
{% macro format_allows_create_or_replace(catalog_relation, existing_relation) %}
  {%- set target_is_managed_iceberg = (
        catalog_relation.table_format == 'iceberg'
        and adapter.get_behavior_flag_no_warn('use_managed_iceberg')
      ) -%}
  {%- set replaceable = (
        (catalog_relation.file_format == 'delta' and existing_relation.is_delta is true)
        or (target_is_managed_iceberg and existing_relation.is_iceberg is true)
      ) -%}
  {{ return(replaceable) }}
{% endmacro %}
