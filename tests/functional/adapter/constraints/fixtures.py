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

expected_sql_v2 = """
create or replace table <model_identifier> (
  `color` string,
  `id` integer not null comment 'hello',
  `date_day` string,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES <foreign_key_model_identifier> (id)
  ) using delta
"""

constraints_yml = fixtures.model_schema_yml.replace("text", "string").replace("primary key", "")

model_fk_constraint_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
          - type: foreign_key
            to: ref('foreign_key_model')
            to_columns: ["id"]
        data_tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        data_tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        data_tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        data_tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: primary_key
"""


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

parent_foreign_key = """
version: 2

models:
  - name: parent_table
    config:
      materialized: table
      on_schema_change: fail
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: not_null
          - type: primary_key
            name: pk_example__parent_table
  - name: child_table
    config:
      materialized: incremental
      on_schema_change: fail
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: pk_example__child_table
        columns: ["id"]
      - type: not_null
        columns: ["id", "name", "parent_id"]
      - type: foreign_key
        name: fk_example__child_table_1
        columns: ["parent_id"]
        to: ref('parent_table')
        to_columns: ["id"]
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: string
      - name: parent_id
        data_type: integer
"""

parent_sql = """
select 1 as id
"""

child_sql = """
 -- depends_on: {{ ref('parent_table') }}

select 2 as id, 'name' as name, 1 as parent_id
"""


my_model_sql = """
{{
  config(
    materialized = "table",
    use_safer_relation_operations = true
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""
