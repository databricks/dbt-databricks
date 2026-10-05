{% macro databricks__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE SQL
{% endmacro %}

{% macro databricks__scalar_function_body_sql() %}
    RETURN
    {{ model.compiled_code }}
{% endmacro %}

{# Python UDF signature macro #}
{% macro databricks__scalar_function_create_replace_signature_python(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql() }})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE PYTHON
    {{ databricks__scalar_function_python_environment() }}
    AS
{% endmacro %}

{# `environment_version` is not in dbt's function config schema, so `config.meta` is also accepted to avoid custom-key warnings. #}
{% macro databricks__scalar_function_python_environment() %}
    {%- set meta = model.config.get('meta') or {} -%}
    {%- set environment = {} -%}
    {%- for key in ['packages', 'environment_version'] -%}
        {%- set config_value = model.config.get(key) -%}
        {%- set meta_value = meta.get(key) -%}
        {%- if config_value and meta_value -%}
            {{ exceptions.raise_compiler_error("`" ~ key ~ "` is configured in both `config." ~ key ~ "` and `config.meta." ~ key ~ "`; use only one.") }}
        {%- endif -%}
        {%- do environment.update({key: config_value or meta_value}) -%}
    {%- endfor -%}
    {%- set packages = environment.packages -%}
    {%- if packages is string -%}
        {%- set packages = [packages] -%}
    {%- endif -%}
    {%- if packages or environment.environment_version %}
    ENVIRONMENT (
      {%- if packages %}
      dependencies = '{{ packages | tojson | replace("'", "''") }}',
      {%- endif %}
      environment_version = '{{ (environment.environment_version or 'None') | string | replace("'", "''") }}'
    )
    {%- endif -%}
{% endmacro %}

{# Python UDF body macro - uses dollar-quoting #}
{% macro databricks__scalar_function_body_python() %}
$$
{{ model.compiled_code }}
$$
{% endmacro %}

{# Main Python UDF macro - combines signature and body #}
{% macro databricks__scalar_function_python(target_relation) %}
    {#- Warn if user explicitly provided no-op config fields -#}
    {%- if model.config.get('runtime_version') -%}
        {{ exceptions.warn("'runtime_version' is accepted for compatibility but has no effect on Databricks Python UDFs. Databricks manages the Python runtime internally.") }}
    {%- endif -%}
    {%- if model.config.get('entry_point') -%}
        {{ exceptions.warn("'entry_point' is accepted for compatibility but has no effect on Databricks Python UDFs. The function body is used directly.") }}
    {%- endif -%}
    {{ databricks__scalar_function_create_replace_signature_python(target_relation) }}
    {{ databricks__scalar_function_body_python() }}
{% endmacro %}
