import os

import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_python_model as fixtures
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.python_model import fixtures as override_fixtures

# Check if ACL tests should be enabled
# Set DBT_ENABLE_ACL_TESTS=1 to enable ACL tests
# Users for tests are set via DBT_TEST_USER_1/2/3 environment variables
pytest.acl_tests_enabled = os.environ.get("DBT_ENABLE_ACL_TESTS") == "1"


def verify_temp_tables_cleaned(project):
    verify_temp_table_cleaned(project, "*__dbt_stg")
    verify_temp_table_cleaned(project, "*__dbt_tmp")


def verify_temp_table_cleaned(project, suffix):
    tmp_tables = project.run_sql(
        "SHOW TABLES IN {database}.{schema} LIKE '" + f"{suffix}'", fetch="all"
    )
    assert len(tmp_tables) == 0


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
# @pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
@pytest.mark.skip("Not available in Databricks yet")
class TestServerlessClusterWithEnvironment(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.serverless_schema_with_environment,
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
@pytest.mark.external
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestComplexConfigV2(TestComplexConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
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


@pytest.mark.python
@pytest.mark.acl
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModelNotebookACL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_notebook_acl.py": override_fixtures.simple_python_model,
            "schema.yml": override_fixtures.notebook_acl_schema,
        }

    def test_python_model_with_notebook_acl(self, project):
        if not pytest.acl_tests_enabled:
            pytest.skip("ACL tests are not enabled")

        result = util.run_dbt(["run"])
        assert len(result) == 1

        sql_results = project.run_sql(
            "SELECT * FROM {database}.{schema}.python_model_with_notebook_acl", fetch="all"
        )
        assert len(sql_results) == 10

        adapter = project.adapter
        conn_mgr = adapter.connections
        api_client = conn_mgr.api_client
        curr_user = api_client.curr_user.get_username()

        username = curr_user
        notebook_path = (
            f"/Users/{username}/dbt_python_models/"
            f"{project.database}/{project.test_schema}/python_model_with_notebook_acl"
        )

        try:
            permissions = api_client.notebook_permissions.get(notebook_path)
            acl_list = permissions.get("access_control_list", [])

            print(f"Notebook ACL list for {notebook_path}: {acl_list}")

            assert any(
                acl.get("user_name") == override_fixtures.TEST_USER_1
                and acl.get("all_permissions")[0]["permission_level"] == "CAN_READ"
                for acl in acl_list
            ), f"{override_fixtures.TEST_USER_1} CAN_READ permission not found"

            assert any(
                acl.get("user_name") == override_fixtures.TEST_USER_2
                and acl.get("all_permissions")[0]["permission_level"] == "CAN_RUN"
                for acl in acl_list
            ), f"{override_fixtures.TEST_USER_2} CAN_RUN permission not found"

            assert any(
                acl.get("user_name") == override_fixtures.TEST_USER_3
                and acl.get("all_permissions")[0]["permission_level"] == "CAN_MANAGE"
                for acl in acl_list
            ), f"{override_fixtures.TEST_USER_3} CAN_MANAGE permission not found"
        except Exception as e:
            print(f"Error getting permissions: {str(e)}")
            raise


# @pytest.mark.python
# @pytest.mark.acl
# @pytest.mark.skip_profile("databricks_uc_sql_endpoint")
# class TestPythonModelNotebookJobACL:
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "python_model_with_notebook_acl.py": override_fixtures.simple_python_model,
#             "schema.yml": override_fixtures.notebook_acl_schema,
#         }

#     def test_python_model_with_notebook_job_acl(self, project):
#         if not pytest.acl_tests_enabled:
#             pytest.skip("ACL tests are not enabled")

#         result = util.run_dbt(["run"])
#         assert len(result) == 1

#         sql_results = project.run_sql(
#             "SELECT * FROM {database}.{schema}.python_model_with_notebook_acl", fetch="all"
#         )
#         assert len(sql_results) == 10

#         adapter = project.adapter
#         conn_mgr = adapter.connections
#         api_client = conn_mgr.api_client

#         try:
#             job_name = f"{project.database}.{project.test_schema}.python_model_with_notebook_acl"
#             jobs = api_client.workflows.search_by_name(job_name)
#             assert len(jobs) == 1, f"Job {job_name} not found"
#             job_id = jobs[0]["job_id"]

#             permissions = api_client.workflow_permissions.get(job_id)
#             acl_list = permissions.get("access_control_list", [])

#             print(f"Job ACL list for job_id {job_id}: {acl_list}")

#             assert any(
#                 acl.get("user_name") == override_fixtures.TEST_USER_1
#                 and acl.get("all_permissions")[0]["permission_level"] == "CAN_VIEW"
#                 for acl in acl_list
#             ), f"{override_fixtures.TEST_USER_1} CAN_VIEW permission not found"

#             assert any(
#                 acl.get("user_name") == override_fixtures.TEST_USER_2
#                 and acl.get("all_permissions")[0]["permission_level"] == "CAN_MANAGE_RUN"
#                 for acl in acl_list
#             ), f"{override_fixtures.TEST_USER_2} CAN_MANAGE_RUN permission not found"

#             assert any(
#                 acl.get("user_name") == override_fixtures.TEST_USER_3
#                 and acl.get("all_permissions")[0]["permission_level"] == "CAN_MANAGE"
#                 for acl in acl_list
#             ), f"{override_fixtures.TEST_USER_3} CAN_MANAGE permission not found"
#         except Exception as e:
#             print(f"Error getting permissions: {str(e)}")
#             raise

@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestChangingSchemaV2(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": override_fixtures.simple_python_model}

    def test_changing_unique_tmp_table_suffix(self, project):
        util.run_dbt(["run"])
        util.write_file(
            override_fixtures.simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        util.run_dbt(["run"])
        verify_temp_tables_cleaned(project)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestChangingSchemaIncrementalV2(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.py": override_fixtures.simple_incremental_python_model}

    def test_changing_unique_tmp_table_suffix(self, project):
        util.run_dbt(["run"])
        util.write_file(
            override_fixtures.simple_incremental_python_model_v2,
            project.project_root + "/models",
            "incremental_model.py",
        )
        util.run_dbt(["run"])
        verify_temp_tables_cleaned(project)
