base_model_sql = """
{{ config(
    materialized = 'table'
) }}
SELECT 'user1' as user_id, 'region_a' as region, CAST(100 AS BIGINT) as amount
"""

base_model_mv = """
{{ config(
    materialized='materialized_view',
) }}
SELECT 'user1' as user_id, 'region_a' as region, CAST(100 AS BIGINT) as amount
"""

row_filter_seed = """
user_id,region,amount
user1,region_a,100
""".strip()

base_model_streaming_table = """
{{ config(
    materialized='streaming_table',
) }}
SELECT * FROM stream {{ ref('base_model_seed') }}
"""

model_with_row_filter = """
version: 2
models:
  - name: base_model
    config:
      row_filter:
        function: region_filter
        columns: [region]
    columns:
      - name: user_id
        data_type: string
      - name: region
        data_type: string
      - name: amount
        data_type: bigint
"""

model_updated_filter = """
version: 2
models:
  - name: base_model
    config:
      row_filter:
        function: user_filter
        columns: [user_id]
    columns:
      - name: user_id
        data_type: string
      - name: region
        data_type: string
      - name: amount
        data_type: bigint
"""

model_no_filter = """
version: 2
models:
  - name: base_model
    columns:
      - name: user_id
        data_type: string
      - name: region
        data_type: string
      - name: amount
        data_type: bigint
"""

# For view failure test
view_model_sql = """
{{ config(
    materialized = 'view'
) }}
SELECT 'user1' as user_id, 'region_a' as region, CAST(100 AS BIGINT) as amount
"""

# For safe_relation_replace path test
base_model_safe_sql = """
{{ config(
    materialized = 'table',
    use_safer_relation_operations = true
) }}
SELECT 'user1' as user_id, 'region_a' as region, CAST(100 AS BIGINT) as amount
"""
