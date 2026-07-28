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

# Incremental model whose schema documents a column absent from the relation. Used to exercise the
# V2 alter/changeset path (ColumnCommentsConfig.get_diff), which — unlike a table rebuild — is only
# reached on a subsequent run against an existing relation.
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

# Create-time coverage for the materializations #1563 did not touch: view, materialized_view,
# streaming_table. Each documents a column absent from the relation; the post-build
# validate_persist_doc_columns check must surface it on create.
missing_column_create_seed = """id,value
1,10
2,20
"""

missing_column_view_sql = """
{{ config(materialized='view') }}
select * from {{ ref('mc_seed') }}
"""

missing_column_view_schema = """
version: 2
models:
  - name: missing_column_view
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""

missing_column_mv_sql = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('mc_seed') }}
"""

missing_column_mv_schema = """
version: 2
models:
  - name: missing_column_mv
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""

missing_column_st_sql = """
{{ config(materialized='streaming_table') }}
select * from stream {{ ref('mc_seed') }}
"""

missing_column_st_schema = """
version: 2
models:
  - name: missing_column_st
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""
