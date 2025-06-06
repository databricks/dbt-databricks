import os

# Use separate test users for ACL testing
TEST_USER_1 = os.environ.get("DBT_TEST_USER_1", "test_user_1")
TEST_USER_2 = os.environ.get("DBT_TEST_USER_2", "test_user_2")
TEST_USER_3 = os.environ.get("DBT_TEST_USER_3", "test_user_3")

# Keep these for backward compatibility
TEST_USER_ACL = TEST_USER_1
TEST_USER_GRANT = TEST_USER_2
TEST_USER_GRANT2 = TEST_USER_3

simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

python_error_model = """
import pandas as pd

def model(dbt, spark):
    raise Exception("This is an error")

    return pd.DataFrame()
"""

# New test case for notebook specific ACL
notebook_acl_schema = f"""version: 2

models:
  - name: python_model_with_notebook_acl
    config:
      create_notebook: true
      user_folder_for_python: true
      python_job_config:
        grants:
          view:
            - user_name: {TEST_USER_1}
          run:
            - user_name: {TEST_USER_2}
          manage:
            - user_name: {TEST_USER_3}
"""

serverless_schema = """version: 2

models:
  - name: my_versioned_sql_model
    versions:
      - v: 1
  - name: my_python_model
    config:
      submission_method: serverless_cluster
      create_notebook: true

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

serverless_schema_with_environment = """version: 2

models:
  - name: my_versioned_sql_model
    versions:
      - v: 1
  - name: my_python_model
    config:
      submission_method: serverless_cluster
      create_notebook: true
      environment_key: "test_key"
      environment_dependencies: ["requests"]

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

workflow_schema = """version: 2

models:
  - name: my_workflow_model
    config:
      submission_method: workflow_job
      user_folder_for_python: true
      python_job_config:
        max_retries: 2
        timeout_seconds: 500
        additional_task_settings: {
          "task_key": "my_dbt_task"
        }
"""

simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
        unique_tmp_table_suffix=True
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
      create_notebook: true
      user_folder_for_python: true

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
    description: This is a python table
    config:
      marterialized: table
      tags: ["python"]
      create_notebook: true
      include_full_name_in_path: true
      location_root: "{{ env_var('DBT_DATABRICKS_LOCATION_ROOT') }}"
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
    dbt.config(unique_key="name")
    dbt.config(liquid_clustered_by="date")
    dbt.config(tblproperties={"a": "b", "c": "d"})
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

simple_incremental_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='incremental',
    )
    data = [[1,2]] * 5
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

simple_incremental_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='incremental',
        unique_tmp_table_suffix=True,
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""
