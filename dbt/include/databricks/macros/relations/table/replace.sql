{% macro safe_relation_replace(existing_relation, staging_relation, intermediate_relation, compiled_code) %}
  
  {{ create_table_at(staging_relation, intermediate_relation, compiled_code) }}

  {{ create_backup(existing_relation) }}

  {{ adapter.rename_relation(staging_relation, existing_relation) }}

  {% call statement('main') %}
    {{ get_drop_backup_sql(existing_relation) }}
  {% endcall %}
  
  {{ adapter.cache_dropped(make_backup_relation(existing_relation, existing_relation.type)) }}

  {{ drop_relation_if_exists(intermediate_relation) }}
{% endmacro %}

{% macro databricks__get_rename_table_sql(relation, new_name) %}
  ALTER TABLE {{ relation }} RENAME TO `{{ relation.database }}`.`{{ relation.schema }}`.`{{ new_name }}`
{% endmacro %}