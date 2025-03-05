seed_csv = """id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

view_sql = """
{{ config(materialized='view') }}
select * from {{ ref('seed') }}
"""

schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
      databricks_tags:
        tag1: value1
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

altered_view_sql = """
{{ config(materialized='view') }}
select id from {{ ref('seed') }}
"""
