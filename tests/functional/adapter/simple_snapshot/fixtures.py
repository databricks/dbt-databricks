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

# Hard deletes test fixtures
snapshot_hard_delete_ignore_sql = """
{% snapshot snapshot_hard_delete_ignore %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='ignore',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""

snapshot_hard_delete_invalidate_sql = """
{% snapshot snapshot_hard_delete_invalidate %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='invalidate',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""

snapshot_hard_delete_new_record_sql = """
{% snapshot snapshot_hard_delete_new_record %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='new_record',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""
