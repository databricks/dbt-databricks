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
