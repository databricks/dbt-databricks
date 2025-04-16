{%- macro create_backup(relation) -%}
  -- get the standard backup name
  {% set backup_relation = make_backup_relation(relation, relation.type) %}

  -- drop any pre-existing backup
  {{ drop_relation(backup_relation) }}

  {{ adapter.rename_relation(relation, backup_relation) }}
{%- endmacro -%}

{%- macro databricks__get_create_backup_sql(relation) -%}

    -- get the standard backup name
    {% set backup_relation = make_backup_relation(relation, relation.type) %}

    -- drop any pre-existing backup
    {{ get_drop_sql(backup_relation) }};

    {{ get_rename_sql(relation, backup_relation.render()) }}

{%- endmacro -%}
