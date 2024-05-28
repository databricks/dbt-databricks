{% macro liquid_clustered_cols() -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    CLUSTER BY (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro apply_liquid_clustered_cols(target_relation) -%}
  {%- set file_format = config.get('file_format', default='delta') -%}
  {%- set partition_by = config.get('partition_by', validator=validation.any[list, basestring], default=None) -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}      
    {%- call statement('set_cluster_by_columns') -%}
        ALTER {{ target_relation.type }} {{ target_relation }} CLUSTER BY (
        {%- for item in cols -%}
            {{ item }}
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
        )
    {%- endcall -%}
  {%- elif not target_relation.is_hive_metastore() and file_format == "delta" and not partition_by -%}
    {%- call statement('unset_cluster_by_columns') -%}
        ALTER {{ target_relation.type }} {{ target_relation }} CLUSTER BY NONE
    {%- endcall -%}
  {%- endif %}
{%- endmacro -%}