from dbt.tests.adapter.constraints import fixtures

# constraints are enforced via 'alter' statements that run after table creation
expected_sql = """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  color,
  date_day
from
( select
    'blue' as color,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""

constraints_yml = fixtures.model_schema_yml.replace("text", "string").replace("primary key", "")

incremental_foreign_key_schema_yml = """
version: 2

models:
  - name: raw_numbers
    config:
      contract:
        enforced: true
      materialized: table
    columns:
        - name: n
          data_type: integer
          constraints:
            - type: primary_key
            - type: not_null
  - name: stg_numbers
    config:
      contract:
        enforced: true
      materialized: incremental
      on_schema_change: append_new_columns
      unique_key: n
    columns:
      - name: n
        data_type: integer
        constraints:
          - type: foreign_key
            name: fk_n
            expression: (n) REFERENCES {schema}.raw_numbers
"""
