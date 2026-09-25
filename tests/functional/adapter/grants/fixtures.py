granted_table_sql = """
{{ config(materialized='table') }}
select 1 as id
"""

granted_table_schema_yml = """
version: 2
models:
  - name: granted_table
    config:
      grants:
        select: ["account users"]
"""
