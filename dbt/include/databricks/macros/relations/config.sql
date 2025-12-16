{%- macro get_configuration_changes(existing_relation) -%}
    {%- set existing_config = adapter.get_relation_config(existing_relation) -%}
    {%- set model_config = adapter.get_config_from_model(config.model) -%}
    {%- set configuration_changes = model_config.get_changeset(existing_config) -%}
    {% do return(configuration_changes) %}
{%- endmacro -%}

{#- Metric view specific config changes - needed because metric views are stored as VIEWs in DB -#}
{%- macro get_metric_view_configuration_changes(existing_relation) -%}
    {#- existing_relation should already be typed as metric_view via incorporate() -#}
    {%- set existing_config = adapter.get_relation_config(existing_relation) -%}
    {%- set model_config = adapter.get_config_from_model(config.model) -%}
    {%- set configuration_changes = model_config.get_changeset(existing_config) -%}
    {% do return(configuration_changes) %}
{%- endmacro -%}