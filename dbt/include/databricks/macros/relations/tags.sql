{% macro apply_tags(relation, set_tags) -%}
  {{ log("Applying tags to relation " ~ set_tags) }}
  {%- if set_tags and relation.is_hive_metastore() -%}
    {{ exceptions.raise_compiler_error("Tags are only supported for Unity Catalog") }}
  {%- endif -%}
  {%- if set_tags and set_tags != [] %}
    {%- call statement('main') -%}
       {{ alter_set_tags(relation, set_tags) }}
    {%- endcall -%}
  {%- endif %}
{%- endmacro -%}

{% macro alter_set_tags(relation, tags) -%}
  ALTER {{ relation.type.render_for_alter() }} {{ relation.render() }} SET TAGS (
    {% for tag in tags -%}
      '{{ tag }}' = '{{ tags[tag] }}' {%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  )
{%- endmacro -%}

{% macro get_set_tag_statements(relation, set_tags, column_tags) -%}
  {%- set statements = [] -%}
  {%- if set_tags -%}
    {%- do statements.append(alter_set_tags(relation, set_tags)) -%}
  {%- endif -%}
  {%- if column_tags and column_tags.set_column_tags -%}
    {%- for column, tags in column_tags.set_column_tags.items() -%}
      {%- do statements.append(alter_set_column_tags(relation, column, tags)) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(statements) }}
{%- endmacro -%}
