-- macros/standardize_column.sql

{% macro standardize_column(column_name, to_lower=true, strip_whitespace=true, remove_special_chars=false) %}
    {%- set expr = column_name -%}

    {%- if strip_whitespace %}
        {%- set expr = "trim(" ~ expr ~ ")" -%}
    {%- endif %}

    {%- if to_lower %}
        {%- set expr = "lower(" ~ expr ~ ")" -%}
    {%- endif %}

    {%- if remove_special_chars %}
        -- Replace non-alphanumeric characters with underscore
        {%- set expr = "regexp_replace(" ~ expr ~ ", '[^a-zA-Z0-9]', '_')" -%}
    {%- endif %}

    {{ expr }}
{% endmacro %}
