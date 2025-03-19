{% macro apply_config_changeset(target_relation, model, configuration_changes) %}
    {{ log("Applying configuration changes to relation " ~ target_relation) }}
    {% if configuration_changes %}
      {% set tags = configuration_changes.changes.get("tags") %}
      {% set tblproperties = configuration_changes.changes.get("tblproperties") %}
      {% set liquid_clustering = configuration_changes.changes.get("liquid_clustering")%}
      {% if tags is not none %}
        {% do apply_tags(target_relation, tags.set_tags, tags.unset_tags) %}
      {%- endif -%}
      {% if tblproperties is not none %}
        {% do apply_tblproperties(target_relation, tblproperties.tblproperties) %}
      {%- endif -%}
      {% if liquid_clustering is not none %}
        {% do apply_liquid_clustered_cols(target_relation, liquid_clustering) %}
      {%- endif -%}
    {%- endif -%}
    {% do persist_docs(target_relation, model, for_relation=True) %}
{% endmacro %}