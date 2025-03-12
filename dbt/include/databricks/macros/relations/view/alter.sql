{% macro alter_view(target_relation, changes) %}
  {{ log("Updating view via ALTER") }}
  {{ adapter.dispatch('alter_view', 'databricks')(target_relation, changes) }}
{% endmacro %}

{% macro databricks__alter_view(target_relation, changes) %}
  {% set tags = changes.get("tags") %}
  {% set tblproperties = changes.get("tblproperties") %}
  {% set query = changes.get("query") %}
  {% if tags %}
    {{ apply_tags(target_relation, tags.set_tags, tags.unset_tags) }}
  {% endif %}
  {% if tblproperties %}
    {{ apply_tblproperties(target_relation, tblproperties.tblproperties) }}
  {% endif %}
  {% if query %}
    {{ alter_query(target_relation, query.query) }}
  {% endif %}
{% endmacro %}
