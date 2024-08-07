comment_schema_yml = """
version: 2
snapshots:
  - name: snapshot
    description: This is a snapshot description
"""

new_comment_schema_yml = """
version: 2
snapshots:
  - name: snapshot
    description: This is a new snapshot description
"""

snapshot_sql = """
{% snapshot snapshot %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['id'],
        )
    }}
    select 1 as id
{% endsnapshot %}
"""
