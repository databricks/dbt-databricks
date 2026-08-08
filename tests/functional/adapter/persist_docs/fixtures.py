_SEEDS__SCHEMA_YML = """
version: 2
seeds:
  - name: persist_seed
    description: 'A seed description'
    config:
      location_root: '{{ env_var("DBT_DATABRICKS_LOCATION_ROOT") }}'
      include_full_name_in_path: true
      persist_docs:
        relation: True
        columns: True
    columns:
      - name: id
        description: 'An id column'
      - name: name
        description: 'A name column'
"""

_HIVE__SCHEMA_YML = """
version: 2
seeds:
  - name: persist_seed
    description: 'A seed description'
    config:
      location_root: '/mnt/dbt_databricks/seeds'
      include_full_name_in_path: true
      persist_docs:
        relation: True
        columns: True
    columns:
      - name: id
        description: 'An id column'
      - name: name
        description: 'A name column'
"""

gate_model_sql = """
{{ config(materialized='table') }}
select 1 as id, 'alice' as name
"""

missing_column_incremental_sql = """
{{ config(materialized='incremental') }}
select 1 as id, 'Ed' as name
"""

missing_column_incremental_schema = """
version: 2
models:
  - name: missing_column_incremental
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""

gate_model_schema = """
version: 2
models:
  - name: gate_model
    description: A described relation
    columns:
      - name: id
        description: The id column description
"""

schema_change_incremental_initial_sql = """
{{ config(materialized='incremental', on_schema_change='append_new_columns') }}
select 1 as id
"""

schema_change_incremental_updated_sql = """
{{ config(materialized='incremental', on_schema_change='append_new_columns') }}
select 1 as id, 'new value' as new_col
"""

schema_change_incremental_initial_yml = """
version: 2
models:
  - name: schema_change_incremental
    columns:
      - name: id
        description: "id comment"
"""

schema_change_incremental_updated_yml = """
version: 2
models:
  - name: schema_change_incremental
    columns:
      - name: id
        description: "id comment"
      - name: new_col
        description: "new column comment"
"""

alter_view_initial_sql = """
{{ config(materialized='view', view_update_via_alter=true) }}
select 1 as id
"""

alter_view_updated_sql = """
{{ config(materialized='view', view_update_via_alter=true) }}
select 1 as id, 2 as added_col
"""

alter_view_initial_yml = """
version: 2
models:
  - name: alter_view
    columns:
      - name: id
        description: "id comment"
"""

alter_view_updated_yml = """
version: 2
models:
  - name: alter_view
    columns:
      - name: id
        description: "updated id comment"
      - name: added_col
        description: "added column comment"
"""
