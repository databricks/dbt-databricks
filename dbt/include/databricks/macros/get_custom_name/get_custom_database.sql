{#
    This is identical to the implementation in dbt-core.
    We need to override because dbt-spark overrides to something we don't like.
#}

{% macro databricks__generate_database_name(custom_database_name=none, node=none) -%}
    {%- if custom_database_name is none -%}
        {{ return(default_database) }}
    {%- else -%}
        {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
        {{ return(catalog_relation.catalog_name) }}
    {%- endif -%}
{%- endmacro %}
