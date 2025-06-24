base_model_sql = """
{{ config(
    materialized = 'table'
) }}
SELECT 'abc123' as id, 'password123' as password;
"""

column_mask_seed = """
id,password
abc123,password123
""".strip()

base_model_streaming_table = """
{{ config(
    materialized='streaming_table',
) }}
SELECT * FROM stream {{ ref('base_model_seed') }}
"""

model = """
version: 2
models:
  - name: base_model
    columns:
      - name: id
        data_type: string
      - name: password
        column_mask:
          function: password_mask
        data_type: string
"""

model_with_extra_args = """
version: 2
models:
  - name: base_model
    columns:
      - name: id
        data_type: string
      - name: password
        data_type: string
        column_mask:
          function: weird_mask
          using_columns: "id, 'literal_string', 333, true, null, INTERVAL 2 DAYS"
"""
