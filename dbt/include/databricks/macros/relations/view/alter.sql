{% macro alter_view(existing_relation, target_relation, sql) %}
  {% set config_changes = get_configuration_changes(existing_relation) %}
  {% if config_changes %}
    {% set tags = configuration_changes.changes.get("tags") %}
    {% set tblproperties = configuration_changes.changes.get("tblproperties") %}
    {% set query = configuration_changes.changes.get("query") %}
    {% if tags %}
      {% do apply_tags(target_relation, tags.set_tags, tags.unset_tags) %}
    {%- endif -%}
    {% if tblproperties %}
      {% do apply_tblproperties(target_relation, tblproperties.tblproperties) %}
    {%- endif -%}
    {%- if query -%}
      {% call statement('main') -%}
  ALTER VIEW {{ target_relation.render() }} AS (
    {{ query }}
  )
      {% endcall %}
    {%- endif -%}
    {% do persist_docs(target_relation, model, for_relation=True) %}
  {% endif %}
{% endmacro %}
