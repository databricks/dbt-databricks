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
