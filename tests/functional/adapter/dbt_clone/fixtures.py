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

contract_view_model_sql = """
{{ config(materialized = 'view') }}

select 1 as id, 'a' as name
"""

contract_view_model_renamed_sql = """
{{ config(materialized = 'view') }}

select 1 as id, 'a' as full_name
"""

contract_view_schema_yml = """
version: 2
models:
  - name: view_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: name
        data_type: string
"""

contract_view_renamed_schema_yml = """
version: 2
models:
  - name: view_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: full_name
        data_type: string
"""
