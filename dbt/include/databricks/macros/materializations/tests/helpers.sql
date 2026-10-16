{% macro databricks__get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}
  {{ dbt.default__get_unit_test_sql(adapter.clean_sql(main_sql), expected_fixture_sql, expected_column_names) }}
{%- endmacro %}
