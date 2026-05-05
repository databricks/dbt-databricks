{% macro alter_view(target_relation, changes) %}
  {{ log("Updating view via ALTER") }}
  {{ adapter.dispatch('alter_view', 'dbt')(target_relation, changes) }}
{% endmacro %}

{% macro databricks__alter_view(target_relation, changes) %}
  {% set tags = changes.get("tags") %}
  {% set tblproperties = changes.get("tblproperties") %}
  {% set query = changes.get("query") %}
  {% set column_comments = changes.get("column_comments") %}
  {% if tags %}
    {{ apply_tags(target_relation, tags.set_tags) }}
  {% endif %}
  {% if tblproperties %}
    {{ apply_tblproperties(target_relation, tblproperties.tblproperties) }}
  {% endif %}
  {% if query %}
    {{ alter_query(target_relation, query.query) }}
    {% if config.persist_column_docs() and model.columns %}
      {#-- ALTER VIEW AS <query> will wipe all column comments in a column_list, so we need to reapply them here. --#}
      {%- set existing_columns = adapter.get_columns_in_relation(target_relation) -%}
      {%- set columns_to_persist = adapter.get_persist_doc_columns(existing_columns, model.columns) -%}
      {{ alter_column_comment(target_relation, columns_to_persist) }}
    {% endif %}
  {% endif %}
{% endmacro %}
