{% materialization table, adapter = 'databricks', supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {% if old_relation %}
    -- setup: if the target relation already exists, drop it
    -- in case if the existing and future table is delta in the same location, we want to do a
    -- create or replace table instead of dropping, so we don't have the table unavailable
    {% set is_delta = (old_relation.is_delta and config.get('file_format', default='delta') == 'delta') %}
    {% set is_same_location = (old_relation.location_root == config.get('location_root', validator=validation.any[basestring])) %}
    {% if not (is_delta and is_same_location) %}
      {{ adapter.drop_relation(old_relation) }}
    {% endif %}
  {%- endif %}

  -- build model

  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do persist_constraints(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
