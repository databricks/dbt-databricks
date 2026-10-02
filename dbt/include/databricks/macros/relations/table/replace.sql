{% macro safe_relation_replace(existing_relation, staging_relation, intermediate_relation, compiled_code) %}
  {#- PK/FK names are schema-unique, so add them only once the old table is dropped. -#}
  {% set key_constraints = create_table_at(staging_relation, intermediate_relation, compiled_code, defer_key_constraints=True) %}

  {{ create_backup(existing_relation) }}

  {{ adapter.rename_relation(staging_relation, existing_relation) }}

  

  {% call statement('main') %}
    {{ get_drop_backup_sql(existing_relation) }}
  {% endcall %}
  
  {{ adapter.cache_dropped(make_backup_relation(existing_relation, existing_relation.type)) }}

  {#- Name keys from the model identifier, as the incremental constraint diff does. -#}
  {% set keyed_relation = this.incorporate(type='table').enrich(key_constraints) %}
  {% for constraint in keyed_relation.create_constraints %}
    {% call statement('add constraint') %}
      ALTER TABLE {{ keyed_relation.render() }} ADD {{ constraint.render() }}
    {% endcall %}
  {% endfor %}

  {{ drop_relation_if_exists(intermediate_relation) }}
{% endmacro %}
