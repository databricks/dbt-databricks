{% macro reconcile_tags(relation, replaced_in_place=false) -%}
  {%- if replaced_in_place -%}
    {%- set changes = adapter.get_table_replacement_tag_changes(relation, config.model) -%}
    {%- set tags = changes['table_tags'] -%}
    {%- set column_tags = {'set_column_tags': changes['column_tags']} -%}
  {%- else -%}
    {%- set tags = config.get('databricks_tags') -%}
    {%- set column_tags = adapter.get_column_tags_from_model(config.model) -%}
  {%- endif -%}
  {%- do apply_tags(relation, tags) -%}
  {%- if column_tags and column_tags.set_column_tags -%}
    {%- do apply_column_tags(relation, column_tags) -%}
  {%- endif -%}
{%- endmacro %}

{% macro fetch_tags(relation) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Tags are only supported for Unity Catalog") }}
  {%- endif %}
  {% call statement('list_tags', fetch_result=True) -%}
    {{ fetch_tags_sql(relation) }}
  {% endcall %}
  {% do return(load_result('list_tags').table) %}
{%- endmacro -%}

{% macro fetch_tags_sql(relation) -%}
  SELECT tag_name, tag_value
  FROM `system`.`information_schema`.`table_tags`
  WHERE catalog_name = '{{ relation.database|lower }}' 
    AND schema_name = '{{ relation.schema|lower }}'
    AND table_name = '{{ relation.identifier|lower }}'
{%- endmacro -%}

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
