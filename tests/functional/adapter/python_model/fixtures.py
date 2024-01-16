simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""

incremental_model = """
import pandas as pd

def model(dbt, spark):
    dbt.config(materialized="incremental")
    dbt.config(unique_key="name")
    dbt.config(on_schema_change="append_new_columns")
    if dbt.is_incremental:
        data = [[2, "Teo", "Mr"], [2, "Fang", "Ms"], [3, "Elbert", "Dr"]]
        pdf = pd.DataFrame(data, columns=["date", "name", "title"])
    else:
        data = [[2, "Teo"], [2, "Fang"], [1, "Elia"]]
        pdf = pd.DataFrame(data, columns=["date", "name"])

    df = spark.createDataFrame(pdf)

    return df
"""

expected_incremental = """date,name,title
1,"Elia",null
2,"Teo","Mr"
2,"Fang","Ms"
3,"Elbert","Dr"
"""

http_path_schema = """version: 2
models:
  - name: my_versioned_sql_model
    versions:
      - v: 1
  - name: my_python_model
    config:
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"

sources:
  - name: test_source
    loader: custom
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: source
"""

complex_schema = """version: 2
models:
  - name: complex_config
    config:
      marterialized: table
      tags: ["python"]
      location_root: '{{ env_var("DBT_DATABRICKS_LOCATION_ROOT") }}'
    columns:
      - name: date
        tests:
          - not_null

      - name: name
        tests:
          - unique
"""

complex_py = """
import pandas as pd  # type: ignore


def model(dbt, spark):
    dbt.config(materialized="incremental")
    dbt.config(partition_by="date")
    dbt.config(unique_key="name")
    if dbt.is_incremental:
        data = [[2, "Teo"], [2, "Fang"], [3, "Elbert"]]
    else:
        data = [[2, "Teo"], [2, "Fang"], [1, "Elia"]]
    pdf = pd.DataFrame(data, columns=["date", "name"])

    df = spark.createDataFrame(pdf)

    return df
"""

expected_complex = """date,name
1,"Elia"
2,"Teo"
2,"Fang"
3,"Elbert"
"""
