{% macro get_create_sql_expectations(expectations) -%}
  {%- set actions = {'warn': '', 'drop': ' ON VIOLATION DROP ROW', 'fail': ' ON VIOLATION FAIL UPDATE'} -%}
  {%- set clauses = [] -%}
  {%- for expectation in expectations.expectations -%}
    {%- do clauses.append('CONSTRAINT ' ~ expectation['name'] ~ ' EXPECT (' ~ expectation['expression'] ~ ')' ~ actions[expectation['on_violation']]) -%}
  {%- endfor -%}
  {{- clauses | join(', ') -}}
{%- endmacro %}
