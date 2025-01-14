{% macro liquid_clustered_cols() -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    CLUSTER BY ({{ cols | join(', ') }})
  {%- endif %}
{%- endmacro -%}

{% macro apply_liquid_clustered_cols(target_relation) -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {%- call statement('set_cluster_by_columns') -%}
        ALTER {{ target_relation.type }} {{ target_relation.render() }} CLUSTER BY ({{ cols | join(', ') }})
    {%- endcall -%}
  {%- endif %}
{%- endmacro -%}