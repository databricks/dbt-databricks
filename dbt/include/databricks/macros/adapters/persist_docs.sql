{% macro databricks__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', default='delta') in ['delta', 'hudi'] %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation }} change column
            {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
            comment '{{ escaped_comment }}';
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro alter_table_comment(relation, model) %}
  {% set comment_query %}
    comment on table {{ relation|lower }} is '{{ model.description | replace("'", "\\'") }}'
  {% endset %}
  {% do run_query(comment_query) %}
{% endmacro %}

{% macro databricks__persist_docs(relation, model, for_relation, for_columns) -%}
  {%- if for_relation and config.persist_relation_docs() and model.description %}
    {% do alter_table_comment(relation, model) %}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
    {%- set columns_to_persist_docs = adapter.get_persist_doc_columns(existing_columns, model.columns) -%}
    {% do alter_column_comment(relation, columns_to_persist_docs) %}
  {% endif %}
{% endmacro %}

{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if column_name in column_dict and column_dict[column_name]["description"] -%}
    {% set escaped_description = column_dict[column_name]["description"] | replace("'", "\\'") %}
    {% set column_comment_clause = "comment '" ~ escaped_description ~ "'" %}
  {%- endif -%}
  {{ adapter.quote(column_name) }} {{ column_comment_clause }}
{% endmacro %}

{% macro get_persist_docs_column_list(model_columns, query_columns) %}
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
{% endmacro %}
