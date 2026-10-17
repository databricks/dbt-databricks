{%- macro escape_comment(comment) -%}
  {#-- escape backslashes first so they cannot merge with the apostrophe escape below --#}
  {{ return(comment | replace("\\", "\\\\") | replace("'", "\\'")) }}
{%- endmacro -%}

{%- macro get_create_sql_comment(comment) -%}
{% if comment is string -%}
  COMMENT '{{ escape_comment(comment) }}'
{%- endif -%}
{%- endmacro -%}
