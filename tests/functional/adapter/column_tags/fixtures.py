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
          key_only: ""
          null_value:
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
          key_only: ""
          null_value:
"""

column_tags_seed = """
id,account_number
1,'1234567890'
""".strip()

base_model_streaming_table = """
SELECT * FROM stream {{ ref('base_model_seed') }}
"""

snapshot_column_tag_sql = """
{% snapshot snapshot %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['account_number'],
        )
    }}
    select 1 as id, 'abc123' as account_number
{% endsnapshot %}
"""

initial_snapshot_column_tag_schema = """
version: 2
snapshots:
  - name: snapshot
    columns:
      - name: id
      - name: account_number
        databricks_tags:
          pii: "true"
          sensitive: "true"
"""

updated_snapshot_column_tag_schema = """
version: 2
snapshots:
  - name: snapshot
    columns:
      - name: id
        databricks_tags:
          pii: "false"
      - name: account_number
        databricks_tags:
          pii: "true"
          sensitive: "true"
"""
