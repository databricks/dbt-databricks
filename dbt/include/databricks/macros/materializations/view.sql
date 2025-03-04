{% materialization view, adapter='databricks' -%}
  {{ log("MATERIALIZING VIEW") }}
  {%- set existing_relation = load_relation_with_metadata(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {% set grant_config = config.get('grants') %}
  {% set tags = config.get('databricks_tags') %}

  {% if adapter.behavior.use_materialization_v2 %}
    {{ run_pre_hooks() }}
    {% if existing_relation %}
      {% set update_via_alter = config.get('view_update_via_alter', False) | as_bool %}
      {% if existing_relation.is_view and update_via_alter %}
        {% if target_relation.is_hive_metastore() %}
          {{ exceptions.raise_compiler_error("Cannot update a view in the Hive metastore via ALTER VIEW. Please set `view_update_via_alter: false` in your model configuration.") }}
        {% endif %}
        {% set configuration_changes = get_configuration_changes(existing_relation) %}
        {% if configuration_changes and not configuration_changes.requires_full_refresh %}
          {{ alter_view(target_relation, configuration_changes) }}
        {% else %}
          {{ replace_with_view(existing_relation, target_relation) }}
        {% endif %}
      {% else %}
        {{ replace_with_view(existing_relation, target_relation) }}
      {% endif %}
    {% else %}
      {% call statement('main') -%}
        {{ get_create_view_as_sql(target_relation, sql) }}
      {%- endcall %}
    {% endif %}
    {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=True) %}

    {{ run_post_hooks() }}

  {% else %}
    {{ run_hooks(pre_hooks) }}

    -- If there's a table with the same name and we weren't told to full refresh,
    -- that's an error. If we were told to full refresh, drop it. This behavior differs
    -- for Snowflake and BigQuery, so multiple dispatch is used.
    {%- if existing_relation is not none and not existing_relation.is_view -%}
      {{ handle_existing_table(should_full_refresh(), existing_relation) }}
    {%- endif -%}

    -- build model
    {% call statement('main') -%}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {%- endcall %}

    {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=True) %}

    {%- do apply_tags(target_relation, tags) -%}

    {{ run_hooks(post_hooks) }}
  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro replace_with_view(existing_relation, target_relation) %}
  {% set tags = config.get('databricks_tags') %}
  {% set safe_create = config.get('safe_table_create', False) | as_bool  %}
  {% if safe_create %}
    {{ log("Trying safe create") }}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {%- set intermediate_relation =  make_staging_relation(target_relation, type='view') -%}
    {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
      -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}
    {% call statement('main') -%}
      {{ get_create_view_as_sql(intermediate_relation, sql) }}
    {%- endcall %}
    {% if existing_relation.is_dlt %}
      {{ drop_relation(existing_relation) }}
    {% else %}
      {{ log("Backing up existing relation") }}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {{ drop_relation_if_exists(backup_relation) }}
  {% else %}
    {{ drop_relation(existing_relation) }}
    {% call statement('main') -%}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {%- endcall %}
  {% endif %}
  {%- do apply_tags(target_relation, tags) -%}
{% endmacro %}