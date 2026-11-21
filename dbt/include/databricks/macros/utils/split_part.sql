{% macro databricks__split_part(string_text, delimiter_text, part_number) %}

    {#- split() takes a regex, so \Q...\E makes it match the delimiter literally. -#}
    {%- set parts -%}
        split({{ string_text }}, concat('\\Q', {{ delimiter_text }}, '\\E'))
    {%- endset -%}

    {%- if part_number >= 0 -%}

        {%- set split_part_expr -%}
        get({{ parts }}, {{ part_number - 1 if part_number > 0 else part_number }})
        {%- endset -%}

    {%- else -%}

        {%- set split_part_expr -%}
        get({{ parts }}, size({{ parts }}) + {{ part_number }})
        {%- endset -%}

    {%- endif -%}

    {{ return(split_part_expr) }}

{% endmacro %}
