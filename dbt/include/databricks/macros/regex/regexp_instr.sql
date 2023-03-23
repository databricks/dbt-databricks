{% macro databricks__regexp_instr(source_value, regexp, position, occurrence, is_raw) %}
regexp_instr( {{ source_value }}, "{{ regexp }}")
{% endmacro %}