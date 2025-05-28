base_model_sql = """
{{ config(
    materialized = 'table'
) }}
SELECT 'abc-123' as id, 'password123' as password;
"""


model = """
version: 2
models:
  - name: base_model
    columns:
      - name: id
        data_type: string
      - name: password
        column_mask: password_mask
        data_type: string
"""
