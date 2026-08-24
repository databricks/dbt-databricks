{% macro location_clause(relation, catalog_facts=none) %}
  {#--
    Moving forward, `relation` should be a `CatalogRelation`, which is covered by the first condition.
    However, there could be existing macros that are still passing in a `BaseRelation`, including user macros.
    Hence, we need to support the old code still, which is covered by the second condition.
  --#}
  {%- if (catalog_facts is not none and catalog_facts.catalog_type is not none)
      or (catalog_facts is none and relation.catalog_type is not none) -%}

    {%- if relation.location is not none -%}
    location '{{ relation.location }}{% if is_incremental() %}_tmp{% endif %}'
    {%- endif -%}

  {%- else -%}

  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set file_format = adapter.resolve_file_format(config) -%}
  {%- set identifier = model['alias'] -%}
  {%- if location_root is not none %}
  {%- set model_path = adapter.compute_external_path(config, model, is_incremental()) %}
    location '{{ model_path }}'
  {%- elif (not relation.is_hive_metastore()) and file_format != 'delta' -%}
    {{ exceptions.raise_compiler_error(
        'Incompatible configuration: `location_root` must be set when using a non-delta file format with Unity Catalog'
    ) }}
  {%- endif %}

  {%- endif %}
{%- endmacro -%}
