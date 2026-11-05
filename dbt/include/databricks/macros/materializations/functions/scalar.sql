{% macro databricks__formatted_scalar_function_args_sql() %}
    {%- set is_python = model.language == 'python' -%}
    {%- set persist_arg_docs = config.persist_column_docs() -%}
    {%- set args = [] -%}
    {%- for arg in model.arguments -%}
        {%- set parts = [arg.name ~ ' ' ~ arg.data_type] -%}
        {%- set default_value = arg.get('default_value') -%}
        {%- if default_value is not none -%}
            {%- if is_python -%}
                {%- do exceptions.warn("Found `default_value` on argument `" ~ arg.name ~ "` of Python function `" ~ model.name ~ "`. Databricks supports DEFAULT only for SQL functions, so it will be ignored") -%}
            {%- else -%}
                {%- do parts.append('DEFAULT ' ~ default_value) -%}
            {%- endif -%}
        {%- endif -%}
        {%- if persist_arg_docs and arg.get('description') -%}
            {%- do parts.append(get_create_sql_comment(arg.get('description')) | trim) -%}
        {%- endif -%}
        {%- do args.append(parts | join(' ')) -%}
    {%- endfor -%}
    {{ args | join(', ') }}
{% endmacro %}

{% macro databricks__function_comment_sql() %}
    {%- if config.persist_relation_docs() and model.description -%}
        {{ get_create_sql_comment(model.description) }}
    {%- endif -%}
{% endmacro %}

{% macro databricks__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE SQL
    {{ databricks__function_comment_sql() }}
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
    {{ databricks__function_comment_sql() }}
    AS
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
