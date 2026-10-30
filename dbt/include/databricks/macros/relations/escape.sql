{% macro escape_relation_literal(value) -%}
  {%- if value is not none -%}
    {{- value | string | replace('\\', '\\\\') | replace("'", "\\'") -}}
  {%- endif -%}
{%- endmacro %}
