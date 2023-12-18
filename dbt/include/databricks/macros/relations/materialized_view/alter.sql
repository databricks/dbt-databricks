{% macro databricks__get_materialized_view_configuration_changes(existing_relation, new_config) -%}
    {% set _existing_materialized_view = adapter.get_relation_config(existing_relation) %}
    {%- set materialized_view = adapter.materialized_view_config_from_model(config.model) -%}
    {%- set _configuration_changes = materialized_view.get_changeset(_existing_materialized_view) -%}
    {% do return(_configuration_changes) %}
{% endmacro -%}

{% macro databricks__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    -- apply a full refresh immediately if needed
    {% if configuration_changes.requires_full_refresh %}

        {{ get_replace_sql(existing_relation, relation,  sql) }}

    -- otherwise apply individual changes as needed
    {% else %}

        {%- set autorefresh = configuration_changes.autorefresh -%}
        {%- if autorefresh -%}{{- log('Applying UPDATE AUTOREFRESH to: ' ~ relation) -}}{%- endif -%}

        alter materialized view {{ relation }}
            auto refresh {% if autorefresh.context %}yes{% else %}no{% endif %}

    {%- endif -%}
{% endmacro %}