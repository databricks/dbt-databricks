{%- macro get_create_sql_comment(comment) -%}
{% if comment is string -%}
  COMMENT '{{ comment }}'
{%- endif -%}
{%- endmacro -%}


{%- macro get_create_column_comment(model) -%}
  {%- if model.columns -%}
    {%- set column_definitions = [] -%}
    {%- for column_name, column in model.columns.items() -%}
      {%- set data_type = column.data_type or 'STRING' -%}
      {%- if column.description -%}
        {%- set column_line = column_name ~ ' ' ~ data_type ~ ' COMMENT ' ~ "'" ~ column.description ~ "'" -%}
      {%- endif -%}

      {%- do column_definitions.append(column_line) -%}
    {% endfor %}
    {{ column_definitions | JOIN(',\n ') }}
  {%- endif -%}
{%- endmacro -%}
