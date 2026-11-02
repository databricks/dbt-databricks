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

row_filter_mv_sql = """
{{ config(materialized = 'materialized_view') }}

select 'a' as region, 1 as id
union all
select 'b' as region, 2 as id
"""

row_filter_seed_csv = """region,id
a,1
b,2
""".strip()

row_filter_st_sql = """
{{ config(materialized = 'streaming_table') }}

select * from stream {{ ref('row_filter_seed') }}
"""

row_filter_schema_yml = """
version: 2
models:
  - name: governed_model
    config:
      row_filter:
        function: region_a_filter
        columns: [region]
"""

column_mask_seed_csv = """id,password
abc123,secret
""".strip()

column_mask_st_sql = """
{{ config(materialized = 'streaming_table') }}

select * from stream {{ ref('column_mask_seed') }}
"""

column_mask_schema_yml = """
version: 2
models:
  - name: governed_model
    columns:
      - name: id
        data_type: string
      - name: password
        data_type: string
        column_mask:
          function: password_mask
"""
