{% materialization dlt_notebook, adapter='databricks' %}
    {% do log("Building dlt pipeline") %}
    {% set target_relation = this.incorporate(type=this.DltNotebook) %}
    {{ dlt_notebook_execute_no_op(target_relation) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{% macro dlt_notebook_execute_no_op(target_relation) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ target_relation,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}