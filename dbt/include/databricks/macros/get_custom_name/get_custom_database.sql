{#
    This is identical to the implementation in dbt-core.
    We need to override because dbt-spark overrides to something we don't like.
#}

{% macro databricks__generate_database_name(custom_database_name=none, node=none) -%}
    {%- if custom_database_name is none -%}
         {%- if node is not none -%}
            {%- set catalog_relation = adapter.build_catalog_relation(node) -%}
            {{ return(catalog_relation.catalog_name) }}
        {%- elif 'config' in target -%}
            {%- set catalog_relation = adapter.build_catalog_relation(target) -%}
            {{ return(catalog_relation.catalog_name) }}
        {%- else -%}
            {{ return(target.database) }}
        {%- endif -%}
    {%- else -%}
       {{ return(custom_database_name) }}
    {%- endif -%}
{%- endmacro %}
