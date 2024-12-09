{% macro safe_relation_replace(existing_relation, staging_relation, intermediate_relation, compiled_code) %}
  {{ create_table_at(staging_relation, intermediate_relation, compiled_code) }}

  {{ create_backup(existing_relation) }}

  {{ adapter.rename_relation(staging_relation, target_relation) }}

  {% call statement('main') %}
    {{ get_drop_backup_sql(existing_relation) }}
  {% endcall %}

  {{ drop_relation_if_exists(intermediate_relation) }}
{% endmacro %}