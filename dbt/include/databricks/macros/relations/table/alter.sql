{% macro apply_config_changeset(target_relation, model, configuration_changes) %}
    {{ log("Applying configuration changes to relation " ~ target_relation) }}
    {% do apply_liquid_clustered_cols(target_relation) %}
    {% if configuration_changes %}
      {% set tags = configuration_changes.changes.get("tags") %}
      {% set tblproperties = configuration_changes.changes.get("tblproperties") %}
      {% if tags is not none %}
        {% do apply_tags(target_relation, tags.set_tags, tags.unset_tags) %}
      {%- endif -%}
      {% if tblproperties is not none %}
        {% do apply_tblproperties(target_relation, tblproperties.tblproperties) %}
      {%- endif -%}
    {%- endif -%}
    {% do persist_docs(target_relation, model, for_relation=True) %}
{% endmacro %}