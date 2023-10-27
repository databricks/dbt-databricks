# macros #
MACROS__CAST_SQL = """
{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro databricks__string_literal(s) %}
    cast('{{ s }}' as STRING)
{% endmacro %}
"""
