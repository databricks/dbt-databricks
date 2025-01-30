import os

import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_python_model as fixtures
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from tests.functional.adapter.python_model import fixtures as override_fixtures


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModel(BasePythonModelTests):
    pass


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonFailureModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_failure_model.py": override_fixtures.python_error_model}

    def test_failure_model(self, project):
        util.run_dbt(["run"], expect_pass=False)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonFailureModelNotebook(TestPythonFailureModel):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonIncrementalModel(BasePythonIncrementalTests):
    pass


@pytest.mark.python
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
        with open(log_file) as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log


@pytest.mark.python
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

        util.check_relations_equal(project.adapter, ["incremental_model", "expected_incremental"])


@pytest.mark.python
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


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestServerlessCluster(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.serverless_schema,
            "my_sql_model.sql": fixtures.basic_sql,
            "my_versioned_sql_model_v1.sql": fixtures.basic_sql,
            "my_python_model.py": fixtures.basic_python,
            "second_sql_model.sql": fixtures.second_sql,
        }


@pytest.mark.python
@pytest.mark.external
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

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            }
        }

    def test_expected_handling_of_complex_config(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["build", "-s", "complex_config"])
        util.run_dbt(["build", "-s", "complex_config"])
        util.check_relations_equal(project.adapter, ["complex_config", "expected_complex"])
        results = project.run_sql(
            "SHOW TBLPROPERTIES {database}.{schema}.complex_config", fetch="all"
        )
        result_dict = {result[0]: result[1] for result in results}
        assert result_dict["a"] == "b"
        assert result_dict["c"] == "d"
        results = project.run_sql(
            "select comment from {database}.information_schema"
            ".tables where table_name = 'complex_config'",
            fetch="all",
        )
        assert results[0][0] == "This is a python table"


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestWorkflowJob:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.workflow_schema,
            "my_workflow_model.py": override_fixtures.simple_python_model,
        }

    def test_workflow_run(self, project):
        util.run_dbt(["run", "-s", "my_workflow_model"])

        sql_results = project.run_sql(
            "SELECT * FROM {database}.{schema}.my_workflow_model", fetch="all"
        )
        assert len(sql_results) == 10
