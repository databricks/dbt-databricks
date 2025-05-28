{% macro fetch_column_masks(relation) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Column masks are not supported for Hive Metastore") }}
  {%- endif %}
  {% call statement('list_column_masks', fetch_result=True) -%}
    {{ fetch_column_masks_sql(relation) }}
  {% endcall %}
  {% do return(load_result('list_column_masks').table) %}
{%- endmacro -%}

{% macro fetch_column_masks_sql(relation) -%}
  SELECT 
    column_name,
    mask_name
  FROM `{{ relation.database|lower }}`.information_schema.column_masks
  WHERE table_catalog = '{{ relation.database|lower }}'
    AND table_schema = '{{ relation.schema|lower }}'
    AND table_name = '{{ relation.identifier|lower }}';
{%- endmacro -%}

{% macro apply_column_masks_from_model_columns(relation) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Column masks are not supported for Hive Metastore") }}
  {%- endif %}
  {{ log("Applying column masks from model to relation " ~ relation) }}
  {% set columns = model.get('columns', {}) %}
  {% for column_name, column_def in columns.items() %}
    {% if column_def is mapping and column_def.get('column_mask') %}
      {%- call statement('main') -%}
        {{ alter_set_column_mask(relation, column_name, column_def.column_mask) }}
      {%- endcall -%}
    {% endif %}
  {% endfor %}
{%- endmacro -%}

{% macro apply_column_masks(relation, column_masks) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Column masks are not supported for Hive Metastore") }}
  {%- endif %}
  {{ log("Applying column masks to relation " ~ relation) }}
  {%- if column_masks.unset_column_mask %}
    {%- for column in column_masks.unset_column_mask -%}
      {%- call statement('main') -%}
        {{ alter_drop_column_mask(relation, column) }}
      {%- endcall -%}
    {%- endfor -%}
  {%- endif %}
  {%- if column_masks.set_column_mask %}
    {%- for column, mask in column_masks.set_column_mask.items() -%}
      {%- call statement('main') -%}
        {{ alter_set_column_mask(relation, column, mask) }}
      {%- endcall -%}
    {%- endfor -%}
  {%- endif %}
{%- endmacro -%}

{% macro alter_drop_column_mask(relation, column) -%}
  ALTER TABLE {{ relation.render() }}
  ALTER COLUMN {{ column }}
  DROP MASK;
{%- endmacro -%}

{% macro alter_set_column_mask(relation, column, mask) -%}
  ALTER TABLE {{ relation.render() }}
  ALTER COLUMN {{ column }}
  SET MASK {{ mask }};
{%- endmacro -%}


