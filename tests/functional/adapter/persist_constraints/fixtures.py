incremental_expected = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-02-01
3,Cathy,2022-03-01
"""

invalid_name_expected = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-02-01
3,,2022-03-01
"""

model_expected = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-02-01
"""

seed = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-02-01
"""

schema_yml = """
version: 2

models:
  - name: table_model
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0
    columns:
      - name: id
      - name: name
        meta:
          constraint: not_null
      - name: date

  - name: incremental_model
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0
    columns:
      - name: id
      - name: name
        meta:
          constraint: not_null
      - name: date

  - name: invalid_check_constraint
    meta:
      constraints:
        - name: invalid_constraint
          condition:

  - name: invalid_column_constraint
    columns:
      - name: id
        meta:
          constraint: invalid

  - name: table_model_disable_constraints
    config:
        persist_constraints: false
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0
    columns:
      - name: id
      - name: name
        meta:
          constraint: not_null
      - name: date

  - name: table_model_contract
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: name
        data_type: string
      - name: date
        data_type: string
"""


incremental_model = """
{{config(materialized='incremental')}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
  where date > (select max(date) from {{ this }})
{% endif %}
"""

base_model = """
{{config(materialized='table')}}

select * from {{ ref('seed') }}
"""

snapshot_schema = """
version: 2

snapshots:
  - name: my_snapshot
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0
    columns:
      - name: name
        meta:
          constraint: not_null
"""

snapshot_sql = """
{% snapshot my_snapshot %}

    {{
        config(
          check_cols=["name", "date"],
          unique_key="id",
          strategy="check",
          target_schema=schema
        )

    }}

    select * from {{ ref('seed') }}

{% endsnapshot %}
"""

insert_invalid_id = """
insert into {database}.{schema}.seed values (0, 'Cathy', '2022-03-01');
"""

insert_invalid_name = """
insert into {database}.{schema}.seed values (3, null, '2022-03-01');
"""
