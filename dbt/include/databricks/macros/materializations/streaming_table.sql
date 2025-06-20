{% materialization streaming_table, adapter='databricks' %}
  {% set existing_relation = load_cached_relation(this) %}
  {% set target_relation = this.incorporate(type=this.StreamingTable) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

{% set build_sql = streaming_table_get_build_sql(existing_relation, target_relation) %}

    {% if build_sql == '' %}
        {{ execute_no_op(target_relation) }}
    {% else %}
        {{ streaming_table_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) }}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro streaming_table_get_build_sql(existing_relation, target_relation) %}
    {% set sql = adapter.clean_sql(sql) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter, refresh data
    {% if existing_relation is none %}
        {% set build_sql = get_create_streaming_table_as_sql(target_relation, sql) %}
    {% elif full_refresh_mode or not existing_relation.is_streaming_table %}
        {% set build_sql = get_replace_sql(existing_relation, target_relation, sql) %}
    {% else %}

        -- get config options
        {% set on_configuration_change = config.get('on_configuration_change') %}
        {% set configuration_changes = get_configuration_changes(existing_relation) %}
        {% if configuration_changes is none %}
            {% set build_sql = refresh_streaming_table(target_relation, sql) %}

        {% elif on_configuration_change == 'apply' %}
            {% set build_sql = get_alter_streaming_table_as_sql(target_relation, configuration_changes, sql, existing_relation, None, None) %}
        {% elif on_configuration_change == 'continue' %}
            {% set build_sql = "" %}
            {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ target_relation ~ "`") }}
        {% elif on_configuration_change == 'fail' %}
            {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ target_relation ~ "`") }}

        {% else %}
            -- this only happens if the user provides a value other than `apply`, 'skip', 'fail'
            {{ exceptions.raise_compiler_error("Unexpected configuration scenario") }}

        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}

{% macro streaming_table_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) %}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set grant_config = config.get('grants') %}

    {{ execute_multiple_statements(build_sql) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% set column_tags = adapter.get_column_tags_from_model(config.model) %}
    {% if column_tags and column_tags.set_column_tags %}
        {{ apply_column_tags(target_relation, column_tags) }}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

{% endmacro %}
