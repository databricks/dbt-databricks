source = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-01-02
"""

target = """
{{config(materialized='table', databricks_compute='alternate_warehouse')}}

select * from {{ ref('source') }}
"""

target2 = """
{{config(materialized='table')}}

select * from {{ ref('source') }}
"""

target3 = """
{{config(materialized='table')}}

select * from {{ ref('source') }}
"""

model_schema = """
version: 2

models:
  - name: target
    columns:
      - name: id
      - name: name
      - name: date
  - name: target2
    config:
      databricks_compute: alternate_warehouse
    columns:
      - name: id
      - name: name
      - name: date
  - name: target3
    columns:
      - name: id
      - name: name
      - name: date
"""

seed_properties = """
version: 2

seeds:
  - name: source
    config:
      databricks_compute: alternate_warehouse2
"""

expected_target = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-01-02
"""

target_snap = """
{% snapshot target_snap %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['id', 'name', 'date'],
      databricks_compute='alternate_warehouse3'
    )
}}
select * from {{ ref('target') }}

{% endsnapshot %}
"""
