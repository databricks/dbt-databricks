import os

import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_python_model as fixtures
from dbt.tests.adapter.python_model.test_python_model import BasePythonIncrementalTests
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests
from tests.functional.adapter.python_model import fixtures as override_fixtures


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModel(BasePythonModelTests):
    pass


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonIncrementalModel(BasePythonIncrementalTests):
    pass


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestChangingSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": override_fixtures.simple_python_model}

    def test_changing_schema_with_log_validation(self, project, logs_dir):
        util.run_dbt(["run"])
        util.write_file(
            override_fixtures.simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        util.run_dbt(["run"])
        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestChangingSchemaIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.py": override_fixtures.incremental_model}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_incremental.csv": override_fixtures.expected_incremental}

    def test_changing_schema_via_incremental(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        util.run_dbt(["run"])

        util.check_relations_equal(
            project.adapter, ["incremental_model", "expected_incremental"]
        )


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestSpecifyingHttpPath(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.http_path_schema,
            "my_sql_model.sql": fixtures.basic_sql,
            "my_versioned_sql_model_v1.sql": fixtures.basic_sql,
            "my_python_model.py": fixtures.basic_python,
            "second_sql_model.sql": fixtures.second_sql,
        }


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestComplexConfig:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_complex.csv": override_fixtures.expected_complex}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.complex_schema,
            "complex_config.py": override_fixtures.complex_py,
        }

    def test_expected_handling_of_complex_config(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["build", "-s", "complex_config"])
        util.run_dbt(["build", "-s", "complex_config"])
        util.check_relations_equal(
            project.adapter, ["complex_config", "expected_complex"]
        )
