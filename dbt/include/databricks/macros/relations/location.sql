{% macro location_clause(relation) %}
  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set file_format = config.get('file_format', default='delta') -%}
  {%- set identifier = model['alias'] -%}
  {%- if location_root is not none %}
    location '{{ location_root }}/{{ identifier }}'
  {%- elif (not relation.is_hive_metastore()) and file_format != 'delta' -%}
    {{ exceptions.raise_compiler_error(
        'Incompatible configuration: `location_root` must be set when using a non-delta file format with Unity Catalog'
    ) }}
  {%- endif %}
{%- endmacro -%}
