base_model_sql = """
select 1 as id, 'abc123' as account_number
"""

initial_column_tag_model = """
version: 2
models:
  - name: base_model
    config:
        materialized: table
    columns:
      - name: id
      - name: account_number
        databricks_tags:
          pii: "true"
          sensitive: "true"
"""

updated_column_tag_model = """
version: 2
models:
  - name: base_model
    config:
        materialized: table
    columns:
      - name: id
        databricks_tags:
          pii: "false"
      - name: account_number
        databricks_tags:
          pii: "true"
          sensitive: "true"
"""

column_tags_seed = """
id,account_number
1,'1234567890'
""".strip()

base_model_streaming_table = """
SELECT * FROM stream {{ ref('base_model_seed') }}
"""
