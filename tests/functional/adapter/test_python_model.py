import os

import pytest

from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.adapter.python_model.test_spark import (
    BasePySparkTests,
    PANDAS_MODEL,
    PANDAS_ON_SPARK_MODEL,
    PYSPARK_MODEL,
)


@pytest.mark.skip_profile("databricks_sql_endpoint", "databricks_uc_sql_endpoint")
class TestPythonModelDatabricks(BasePythonModelTests):
    pass


@pytest.mark.skip_profile("databricks_sql_endpoint", "databricks_uc_sql_endpoint")
class TestPythonIncrementalModelDatabricks(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}


models__simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""
models__simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""


@pytest.mark.skip_profile("databricks_sql_endpoint", "databricks_uc_sql_endpoint")
class TestChangingSchemaDatabricks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": models__simple_python_model}

    def test_changing_schema_with_log_validation(self, project, logs_dir):
        run_dbt(["run"])
        write_file(
            models__simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        run_dbt(["run"])
        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log


@pytest.mark.skip_profile("databricks_sql_endpoint", "databricks_uc_sql_endpoint")
class TestPySparkDatabricks(BasePySparkTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pandas_df.py": PANDAS_MODEL,
            "pyspark_df.py": PYSPARK_MODEL,
            "pandas_on_spark_df.py": PANDAS_ON_SPARK_MODEL,
        }

    def test_different_dataframes(self, project):
        # test
        results = run_dbt(["run"])
        assert len(results) == 3
