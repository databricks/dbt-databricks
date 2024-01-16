{% macro optimize(relation) %}
  {{ return(adapter.dispatch('optimize', 'dbt')(relation)) }}
{% endmacro %}

{%- macro databricks__optimize(relation) -%}
  {%- if var('DATABRICKS_SKIP_OPTIMIZE', 'false')|lower != 'true' and 
        var('databricks_skip_optimize', 'false')|lower != 'true' and 
        config.get('file_format', 'delta') == 'delta' -%}
    {%- if (config.get('zorder', False) or config.get('liquid_clustered_by', False)) -%}
      {%- call statement('run_optimize_stmt') -%}
        {{ get_optimize_sql(relation) }}
      {%- endcall -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}

{%- macro get_optimize_sql(relation) %}
  optimize {{ relation }}
  {%- if config.get('zorder', False) and config.get('file_format', 'delta') == 'delta' %}
    {%- if config.get('liquid_clustered_by', False) %}
      {{ exceptions.warn("Both zorder and liquid_clustered_by are set but they are incompatible. zorder will be ignored.") }}
    {%- else %}
      {%- set zorder = config.get('zorder', none) %}
      {# TODO: predicates here? WHERE ...  #}
      {%- if zorder is sequence and zorder is not string %}
        zorder by (
        {%- for col in zorder %}
        {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
        )
      {%- else %}
        zorder by ({{zorder}})
      {%- endif %}
    {%- endif %}
  {%- endif %}
{%- endmacro -%}