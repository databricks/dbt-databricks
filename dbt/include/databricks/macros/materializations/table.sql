{% materialization table, adapter = 'databricks', supported_languages=['sql', 'python'] %}
  {{ log("MATERIALIZING TABLE") }}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- set tags = config.get('databricks_tags') -%}
  {%- set safe_create = config.get('safe_table_create', True) %}
  {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) %}
  {% set target_relation = this.incorporate(type='table') %}

  {% if adapter.behavior.use_materialization_v2 %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set staging_relation = make_staging_relation(target_relation) %}

    {{ run_pre_hooks() }}
    
    {% call statement('main', language=language) %}
      {{ get_create_intermediate_table(intermediate_relation, compiled_code, language) }}
    {% endcall %}
    {% if not existing_relation %}
      {{ create_table_at(target_relation, intermediate_relation, compiled_code) }}
    {% else %}
      {% if safe_create and existing_relation.can_be_renamed %}
        {{ safe_relation_replace(existing_relation, staging_relation, intermediate_relation, compiled_code) }}
      {% else %}
        {% if existing_relation and not (existing_relation.can_be_replaced and config.get('file_format', default='delta') == 'delta') -%}
          {{ adapter.drop_relation(existing_relation) }}
        {%- endif %}
        {{ create_table_at(target_relation, intermediate_relation, compiled_code) }}
      {% endif %}
    {% endif %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {{ apply_grants(target_relation, grant_config, should_revoke) }}
    
    {{ run_post_hooks() }}
  {% else %}
    {{ run_hooks(pre_hooks) }}
    -- setup: if the target relation already exists, drop it
    -- in case if the existing and future table is delta, we want to do a
    -- create or replace table instead of dropping, so we don't have the table unavailable
    {% if existing_relation and not (existing_relation.can_be_replaced and config.get('file_format', default='delta') == 'delta') -%}
      {{ adapter.drop_relation(existing_relation) }}
    {%- endif %}

    -- build model

    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
    {% if language=="python" %}
      {% do apply_tblproperties(target_relation, tblproperties) %}
    {% endif %}
    {%- do apply_tags(target_relation, tags) -%}

    {% do persist_docs(target_relation, model, for_relation=language=='python') %}

    {% do persist_constraints(target_relation, model) %}

    {% do optimize(target_relation) %}

    {{ run_hooks(post_hooks) }}

  {% endif %}
  {{ return({'relations': [target_relation]})}}
{% endmaterialization %}
