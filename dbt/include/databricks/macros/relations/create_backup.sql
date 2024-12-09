{%- macro create_backup(relation) -%}

    -- get the standard backup name
    {% set backup_relation = make_backup_relation(relation, relation.type) %}

    -- drop any pre-existing backup
    {% call statement('drop existing backup') %}
      {{ get_drop_sql(backup_relation) }}
    {% endcall %}

    {% call statement('backing up') %}
      {{ get_rename_sql(relation, backup_relation.identifier) }}
    {% endcall %}

{%- endmacro -%}