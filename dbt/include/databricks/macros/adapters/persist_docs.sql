{% macro databricks__alter_column_comment(relation, column_dict, renderer_variant=none) %}
  {% set supported = renderer_variant in ['comment_on_column', 'alter_column'] %}
  {% set file_format = none %}
  {% if renderer_variant is none %}
    {% set file_format = adapter.resolve_file_format(config) %}
    {% set supported = file_format in ['delta', 'hudi'] %}
  {% endif %}
  {% if supported %}
    {% for column in column_dict.values() %}
      {% set comment = column['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set column_path = relation.render() ~ '.' ~ adapter.quote(column['name']) %}
      {{ run_query_as(comment_on_column_sql(column_path, escaped_comment, renderer_variant), 'alter_column_comment', fetch_result=False) }}
    {% endfor %}
  {% else %}
    {% if renderer_variant is none %}
      {{ log('WARNING - requested to update column comments, but file format ' ~ file_format ~ ' does not support that.') }}
    {% else %}
      {{ log('WARNING - planned table provider does not support updating column comments.') }}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro comment_on_column_sql(column_path, escaped_comment, renderer_variant=none) %}
  {%- if renderer_variant == 'comment_on_column' -%}
    COMMENT ON COLUMN {{ column_path }} IS '{{ escaped_comment }}'
  {%- elif renderer_variant == 'alter_column' -%}
    {{ alter_table_change_column_comment_sql(column_path, escaped_comment) }}
  {%- elif renderer_variant is none and adapter.has_dbr_capability('comment_on_column') -%}
    COMMENT ON COLUMN {{ column_path }} IS '{{ escaped_comment }}'
  {%- else -%}
    {{ alter_table_change_column_comment_sql(column_path, escaped_comment) }}
  {%- endif -%}
{% endmacro %}

{% macro alter_table_change_column_comment_sql(column_path, escaped_comment) %}
  {%- set parts = column_path.split('.') -%}
  {%- if parts|length >= 4 -%}
    {%- set table_path = parts[:-1] | join('.') -%}
    {%- set column_name = parts[-1] -%}
    ALTER TABLE {{ table_path }} ALTER COLUMN {{ column_name }} COMMENT '{{ escaped_comment }}'
  {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid column path: " ~ column_path ~ ". Expected format: database.schema.table.column") }}
  {%- endif -%}
{% endmacro %}

{% macro databricks__persist_docs(relation, model, for_relation, for_columns, column_comment_renderer_variant=none) -%}
  {%- if for_relation and config.persist_relation_docs() and model.description %}
    {{ run_query_as(alter_relation_comment_sql(relation, model.description), 'alter_relation_comment', fetch_result=False) }}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
    {%- set columns_to_persist_docs = adapter.get_persist_doc_columns(existing_columns, model.columns) -%}
    {{ alter_column_comment(relation, columns_to_persist_docs, column_comment_renderer_variant) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_comment_sql(relation, description) %}
COMMENT ON {{ relation.type.render().upper() }} {{ relation.render() }} IS '{{ description | replace("'", "\\'") }}'
{% endmacro %}

{% macro alter_column_comments(relation, column_dict) %}
  {% for column, comment in column_dict.items() %}
    {{ log('Updating comment for column ' ~ column ~ ' with comment ' ~ comment) }}
    {% set escaped_comment = comment | replace('\'', '\\\'') %}
    {% set column_path = relation.render() ~ '.' ~ adapter.quote(column) %}
    {{ run_query_as(comment_on_column_sql(column_path, escaped_comment), 'main', fetch_result=False) }}
  {% endfor %}
{% endmacro %}
