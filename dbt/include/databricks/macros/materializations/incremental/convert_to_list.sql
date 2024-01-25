{% macro convert_to_list(v, default_value=none) %}
    {% if v is none %}
        {{ return(default_value) }}
    {% elif v is sequence and v is not mapping and v is not string %}
        {{ return(v) }}
    {% else %}
        {{ return([v]) }}
    {% endif %}
{% endmacro %}