{# executes a query and explicitly drops the staging table. #}
{% macro statement_with_staging_table(name=None, staging_table=None, fetch_result=False, auto_begin=True) -%}
  {%- if execute: -%}
    {%- set sql = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set res, table = adapter.execute(sql, auto_begin=auto_begin, fetch=fetch_result, staging_table=staging_table) -%}
    {%- if name is not none -%}
      {{ store_result(name, response=res, agate_table=table) }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}

{#- Multi-statement strategies (e.g. the delete+insert DBR<17.1 fallback) build any
    preparatory statements first and their real data-writing statement last, so the
    last statement in the list is treated as authoritative and named 'main' — its
    rowcount becomes the model's adapter response. Earlier statements are executed
    for their side effects only and are not surfaced to run_results.json. -#}
{% macro execute_multiple_statements(statements) %}
  {%- if statements is string %}
    {% call statement(name="main") %}
      {{ statements }}
    {% endcall %}
  {%- else %}
    {%- for sql in statements %}
      {% call statement(name="main" if loop.last else "statement_" ~ loop.index) %}
        {{ sql }}
      {% endcall %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{# a user-friendly interface into statements #}
{% macro run_query_as(sql, name, fetch_result=True) %}
  {% call statement(name, fetch_result, auto_begin=False) %}
    {{ sql }}
  {% endcall %}

  {% if fetch_result %}
    {{ return(load_result(name).table) }}
  {% endif %}
{% endmacro %}