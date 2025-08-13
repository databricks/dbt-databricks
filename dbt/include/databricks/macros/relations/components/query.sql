{% macro alter_query(target_relation, query) %}
  {{ log("Altering query") }}
  {% if query %}
    {% call statement('main') %}
      {{- get_alter_query_sql(target_relation, query) }}
    {% endcall %}
  {% endif %}
{% endmacro %}

{% macro get_alter_query_sql(target_relation, query) -%}
  ALTER {{ target_relation.type.render() }} {{ target_relation.render() }} AS (
    {{ query }}
  )
{%- endmacro %}