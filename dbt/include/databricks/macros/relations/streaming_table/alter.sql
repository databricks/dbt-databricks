{% macro get_alter_streaming_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{- log('Applying ALTER to: ' ~ relation) -}}
    {%- do return(adapter.dispatch('get_alter_streaming_table_as_sql', 'dbt')(
        relation,
        configuration_changes,
        sql,
        existing_relation,
        backup_relation,
        intermediate_relation
    )) -%}
{% endmacro %}


{% macro get_streaming_table_configuration_changes(existing_relation, new_config) -%}
    {{- log('Determining configuration changes on: ' ~ existing_relation) -}}
    {%- do return(adapter.dispatch('get_streaming_table_configuration_changes', 'dbt')(existing_relation, new_config)) -%}
{%- endmacro %}

{%- macro databricks__get_streaming_table_configuration_changes(existing_relation, new_config) -%}
    {%- set _existing_streaming_table = adapter.get_relation_config(existing_relation) -%}
    {%- set streaming_table = adapter.streaming_table_config_from_model(config.model) -%}
    {%- set _configuration_changes = streaming_table.get_changeset(_existing_streaming_table) -%}
    {% do return(_configuration_changes) %}
{%- endmacro -%}

{% macro databricks__get_alter_streaming_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    -- apply a full refresh immediately if needed
    {% if configuration_changes.requires_full_refresh %}
        {% do return(get_replace_sql(existing_relation, relation,  sql)) %}

    -- otherwise apply individual changes as needed
    {% else %}
        {% do return(get_alter_st_internal(relation, configuration_changes, sql)) %}
    {%- endif -%}
{% endmacro %}

{% macro get_alter_st_internal(relation, configuration_changes, sql) %}
  {%- set partition_by = configuration_changes.changes["partition_by"] -%}
  {%- set tblproperties = configuration_changes.changes["tblproperties"] -%}
  {%- set comment = configuration_changes.changes["comment"] -%}
  {%- set refresh = configuration_changes.changes["refresh"] -%}

  CREATE OR REFRESH STREAMING TABLE {{ relation }}
    {%- if partition_by -%}
        {{ get_create_sql_partition_by(partition_by.data.partition_by) }}
    {%- endif -%}
    {%- if comment -%}
        {{ get_create_sql_comment(comment.data.comment) }}
    {%- endif -%}
    {%- if tblproperties -%}
        {{ get_create_sql_tblproperties(tblproperties.data.tblproperties) }}
    {%- endif -%}
    {%- if refresh -%}
        {{ get_create_sql_refresh_schedule(refresh.data.cron, refresh.data.time_zone_value) }}
    {%- endif -%}
    AS {{ sql }}
{% endmacro %}