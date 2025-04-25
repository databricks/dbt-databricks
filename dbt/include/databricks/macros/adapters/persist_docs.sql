{% macro databricks__alter_column_comment(relation, column_dict) %}
  {% set file_format = config.get('file_format', default='delta') %}
  {% if file_format in ['delta', 'hudi'] %}
    {% for column in column_dict.values() %}
      {% set comment = column['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set column_path = relation.render() ~ '.' ~ api.Column.get_name(column) %}
      {{ run_query_as(comment_on_column_sql(column_path, escaped_comment), 'alter_column_comment', fetch_result=False) }}
    {% endfor %}
  {% else %}
    {{ log('WARNING - requested to update column comments, but file format ' ~ file_format ~ ' does not support that.') }}
  {% endif %}
{% endmacro %}

{% macro comment_on_column_sql(column_path, escaped_comment) %}
COMMENT ON COLUMN {{ column_path }} IS '{{ escaped_comment }}'
{% endmacro %}

{% macro databricks__persist_docs(relation, model, for_relation, for_columns) -%}
  {%- if for_relation and config.persist_relation_docs() and model.description %}
    {{ run_query_as(alter_relation_comment_sql(relation, model.description), 'alter_relation_comment', fetch_result=False) }}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
    {%- set columns_to_persist_docs = adapter.get_persist_doc_columns(existing_columns, model.columns) -%}
    {{ alter_column_comment(relation, columns_to_persist_docs) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_comment_sql(relation, description) %}
COMMENT ON {{ relation.type.upper() }} {{ relation.render() }} IS '{{ description | replace("'", "\\'") }}'
{% endmacro %}

{% macro alter_column_comments(relation, column_dict) %}
  {% for column, comment in column_dict.items() %}
    {{ log('Updating comment for column ' ~ column ~ ' with comment ' ~ comment) }}
    {% set escaped_comment = comment | replace('\'', '\\\'') %}
    {% set column_path = relation.render() ~ '.' ~ column %}
    {{ run_query_as(comment_on_column_sql(column_path, escaped_comment), 'main', fetch_result=False) }}
  {% endfor %}
{% endmacro %}