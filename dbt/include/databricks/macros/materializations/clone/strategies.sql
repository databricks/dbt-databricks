{% macro databricks__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace
    table {{ this_relation.render() }}
    shallow clone {{ defer_relation.render() }}
{% endmacro %}

{% macro clone_requires_drop(existing_relation) %}
  {#-
    Drop non-tables and known UC tables that cannot be replaced by a shallow clone.
    Keep shallow clones and HMS tables with unknown type; dropping HMS resets Delta history.
  -#}
  {%- if existing_relation is none -%}
    {{ return(False) }}
  {%- elif not existing_relation.is_table -%}
    {{ return(True) }}
  {%- elif existing_relation.is_shallow_clone -%}
    {{ return(False) }}
  {%- elif existing_relation.databricks_table_type is none -%}
    {{ return(False) }}
  {%- else -%}
    {{ return(True) }}
  {%- endif -%}
{% endmacro %}

{% macro create_or_replace_clone_external(this_relation, defer_relation) %}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    create or replace
    table {{ this_relation.render() }}
    shallow clone {{ defer_relation.render() }}
    {{ location_clause(catalog_relation) }}

{% endmacro %}
