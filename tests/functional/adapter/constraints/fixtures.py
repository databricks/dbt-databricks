from dbt.tests.adapter.constraints import fixtures

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


column_constraint_gate_parent_sql = "select cast(1 as int) as id"

_column_constraint_gate_parent_model_yml = """\
  - name: parent_table
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
            name: pk_parent_table
"""

column_constraint_gate_child_sql = """
select
  cast(x'00' as binary) as hashkey,
  cast('2026-01-01' as timestamp) as load_timestamp,
  cast('seed' as string) as record_source,
  cast(1 as int) as id
"""

column_constraint_gate_child_schema_yml = f"""
version: 2
models:
{_column_constraint_gate_parent_model_yml}  - name: child_table
    config:
      materialized: table
    constraints:
      - type: primary_key
        name: pk_child_table
        columns: ["hashkey", "load_timestamp"]
        warn_unsupported: false
    columns:
      - name: hashkey
        constraints:
          - type: not_null
      - name: load_timestamp
        constraints:
          - type: not_null
      - name: record_source
      - name: id
        constraints:
          - type: foreign_key
            name: fk_child_table_id
            to: ref('parent_table')
            to_columns: [id]
"""

column_constraint_gate_child_with_contract_sql = """
{{ config(materialized='incremental') }}
select
  cast(x'00' as binary) as hashkey,
  cast('2026-01-01' as timestamp) as load_timestamp,
  cast('seed' as string) as record_source,
  cast(1 as int) as id
"""

column_constraint_gate_child_with_contract_schema_yml = f"""
version: 2
models:
{_column_constraint_gate_parent_model_yml}  - name: child_with_contract
    config:
      materialized: incremental
      on_schema_change: append_new_columns
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: pk_child_with_contract
        columns: ["hashkey", "load_timestamp"]
        warn_unsupported: false
    columns:
      - name: hashkey
        data_type: binary
        constraints:
          - type: not_null
      - name: load_timestamp
        data_type: timestamp
        constraints:
          - type: not_null
      - name: record_source
        data_type: string
      - name: id
        data_type: int
        constraints:
          - type: foreign_key
            name: fk_child_with_contract_id
            to: ref('parent_table')
            to_columns: [id]
"""


# Contract-enforced table child carrying PRIMARY KEY + FOREIGN KEY + NOT NULL, so all
# three constraint kinds can be observed in information_schema after create. Reuses the
# column_constraint_gate parent (a table with a PK to satisfy the FK reference).
v1_contract_child_table_sql = """
{{ config(materialized='table') }}
select
  cast(1 as int) as id,
  cast('name' as string) as name,
  cast(1 as int) as parent_id
"""

v1_contract_child_table_schema_yml = f"""
version: 2
models:
{_column_constraint_gate_parent_model_yml}  - name: v1_contract_child
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: pk_v1_contract_child
        columns: ["id"]
      - type: foreign_key
        name: fk_v1_contract_child_parent
        columns: ["parent_id"]
        to: ref('parent_table')
        to_columns: ["id"]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: name
        data_type: string
        constraints:
          - type: not_null
      - name: parent_id
        data_type: int
        constraints:
          - type: not_null
"""


# Contract-enforced `type: custom` constraint, rendered verbatim as
# `add constraint <name> <expression>` and observable as a delta.constraints.* property.
custom_constraint_model_sql = """
{{ config(materialized="table") }}
select 1 as id, 'blue' as color
"""

custom_constraint_schema_yml = """
version: 2
models:
  - name: custom_constraint_model
    config:
      contract:
        enforced: true
    constraints:
      - type: custom
        name: custom_id_positive
        expression: "CHECK (id > 0)"
    columns:
      - name: id
        data_type: int
      - name: color
        data_type: string
"""

incremental_rely_pk_cascade_schema_yml = """
version: 2
models:
  - name: rely_parent
    config:
      materialized: incremental
      unique_key: n
      on_schema_change: append_new_columns
      contract:
        enforced: true
    columns:
      - name: n
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
            name: pk_rely_parent
            expression: RELY
  - name: rely_child
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        name: fk_rely_child
        columns: ["parent_n"]
        to: ref('rely_parent')
        to_columns: ["n"]
    columns:
      - name: parent_n
        data_type: int
        constraints:
          - type: not_null
      - name: child_id
        data_type: int
"""

incremental_rely_pk_parent_sql = """
select 1 as n
"""

incremental_rely_pk_child_sql = """
-- depends_on: {{ ref('rely_parent') }}

select 1 as parent_n, 10 as child_id
"""


def _incremental_contract_off_pk_schema_yml(enforced):
    return f"""
version: 2
models:
  - name: contract_off_pk
    config:
      materialized: incremental
      unique_key: id
      on_schema_change: append_new_columns
      contract:
        enforced: {enforced}
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
            name: pk_contract_off
"""


incremental_contract_off_pk_enforced_schema_yml = _incremental_contract_off_pk_schema_yml("true")
incremental_contract_off_pk_unenforced_schema_yml = _incremental_contract_off_pk_schema_yml("false")

incremental_contract_off_pk_sql = """
select 1 as id
"""
