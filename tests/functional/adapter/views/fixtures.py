seed_csv = """id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

view_sql = """
{{ config(materialized='view') }}
select * from {{ ref('seed') }};
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

no_tag_schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

hive_schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

altered_view_sql = """
{{ config(materialized='view') }}
select id from {{ ref('seed') }};
"""


view_without_tags_sql = """
{{ config(materialized='view') }}

select cast(1 as bigint) as id
"""

view_with_tags_sql = """
{{ config(
    materialized='view',
    databricks_tags={'classification': 'internal'}
) }}

select cast(1 as bigint) as id
"""
