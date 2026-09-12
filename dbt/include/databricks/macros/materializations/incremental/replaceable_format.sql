{#-- True when `create or replace table` can stand in for drop-then-create on a full refresh.

     Managed Iceberg needs its own arm rather than reusing `file_format`: an Iceberg model keeps
     `file_format` at delta (`iceberg_table_properties` raises for anything else) and `iceberg` is
     not an accepted `file_format` at all, so the `file_format == 'iceberg'` test this replaced
     could never be true. The target is Iceberg exactly when `table_format` is iceberg and the
     behavior flag is on -- the same condition `file_format_clause` uses to emit `using iceberg`.
     The flag is read without warning because this runs for every incremental model, including
     projects that never opt in (issue #1266).

     The two arms are mutually exclusive because `create or replace` cannot change a table's
     provider: Databricks rejects it with `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED` and leaves the
     table as it was. A managed-Iceberg target over a legacy Delta table -- a project that has just
     switched the flag on -- must therefore drop and recreate, even though `file_format` still reads
     delta for it. --#}
{% macro format_allows_create_or_replace(catalog_relation, existing_relation) %}
  {%- set target_is_managed_iceberg = (
        catalog_relation.table_format == 'iceberg'
        and adapter.get_behavior_flag_no_warn('use_managed_iceberg')
      ) -%}
  {%- if target_is_managed_iceberg -%}
    {%- set replaceable = existing_relation.is_iceberg is true -%}
  {%- else -%}
    {%- set replaceable = (
          catalog_relation.file_format == 'delta' and existing_relation.is_delta is true
        ) -%}
  {%- endif -%}
  {{ return(replaceable) }}
{% endmacro %}
