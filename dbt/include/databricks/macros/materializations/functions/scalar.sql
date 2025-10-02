{% macro databricks__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.return_type.type }}
    LANGUAGE SQL
{% endmacro %}

{% macro databricks__scalar_function_body_sql() %}
    RETURN
    {{ model.compiled_code }}
{% endmacro %}