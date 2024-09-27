comment_schema_yml = """
version: 2
models:
  - name: view_model
    description: This is a view model
  - name: table_model
    description: This is a table model
"""

view_model_sql = """
{{ config(materialized = 'view') }}

select 1 as id
"""

table_model_sql = """
{{ config(materialized = 'table') }}

select 1 as id
"""
