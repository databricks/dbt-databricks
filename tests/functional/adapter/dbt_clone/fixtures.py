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

auto_cluster_table_model_sql = """
{{ config(materialized = 'table', auto_liquid_cluster = true) }}

select 1 as id
"""

auto_cluster_incremental_model_sql = """
{{ config(materialized = 'incremental', auto_liquid_cluster = true) }}

select 1 as id
"""

tagged_table_model_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'classification': 'internal'},
) }}

select 1 as id
"""

tagged_table_model_schema_yml = """
version: 2

models:
  - name: tagged_table_model
    columns:
      - name: id
        databricks_tags:
          pii: 'false'
"""
