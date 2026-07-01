tags_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'a': 'b', 'c': 'd', 'k': ''},
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
"""

updated_tags_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'e': 'f'},
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
"""

tags_merged_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'c': 'd', 'k': ''},
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
"""

streaming_table_tags_sql = """
{{ config(
    materialized='streaming_table',
    databricks_tags = {'a': 'b', 'c': 'd', 'k': ''},
) }}

select * from stream {{ ref('my_seed') }}
"""

updated_streaming_table_tags_sql = """
{{ config(
    materialized='streaming_table',
    databricks_tags = {'e': 'f'},
) }}

select * from stream {{ ref('my_seed') }}
"""

simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
        submission_method='serverless_cluster',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

python_schema = """version: 2
models:
  - name: tags
    config:
      tags: ["python"]
      databricks_tags:
        a: b
        c: d
        k: ""
"""

snapshot_tags_sql = """
{% snapshot tags_snapshot %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            databricks_tags={'a': 'b', 'c': 'd', 'k': ''},
        )
    }}
    select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
{% endsnapshot %}
"""

# A plain table model; tag config comes from the schema fixtures below.
metadata_fetch_table_sql = """
{{ config(
    materialized = 'table',
) }}

select cast(1 as bigint) as id
"""

metadata_fetch_no_tags_schema = """
version: 2

models:
  - name: metadata_fetch_table
    columns:
      - name: id
"""

metadata_fetch_table_tags_schema = """
version: 2

models:
  - name: metadata_fetch_table
    config:
      databricks_tags:
        classification: internal
    columns:
      - name: id
"""

metadata_fetch_column_tags_schema = """
version: 2

models:
  - name: metadata_fetch_table
    columns:
      - name: id
        databricks_tags:
          classification: internal
"""

# A view, later reconfigured to a tagged table to force a drop+recreate.
metadata_fetch_view_first_sql = """
{{ config(
    materialized = 'view',
) }}

select cast(1 as bigint) as id
"""

metadata_fetch_table_with_tags_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'classification': 'internal'},
) }}

select cast(1 as bigint) as id
"""
