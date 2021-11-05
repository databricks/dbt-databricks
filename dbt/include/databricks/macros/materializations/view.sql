{% materialization view, adapter='databricks' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
