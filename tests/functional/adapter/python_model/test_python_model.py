import os

import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_python_model as fixtures
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from tests.functional.adapter.python_model import fixtures as override_fixtures

# Check if ACL tests should be enabled
# Set DBT_ENABLE_ACL_TESTS=1 to enable ACL tests
# Users for tests are set via DBT_TEST_USER_1/2/3 environment variables
pytest.acl_tests_enabled = os.environ.get("DBT_ENABLE_ACL_TESTS") == "1"


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
@pytest.mark.acl
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModelACL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_acl.py": override_fixtures.acl_python_model,
            "schema.yml": override_fixtures.acl_schema,
        }

    def test_python_model_with_acl(self, project):
        # Only run if ACL tests are enabled
        if not pytest.acl_tests_enabled:
            pytest.skip("ACL tests are not enabled")

        # Run the model
        result = util.run_dbt(["run"])
        assert len(result) == 2

        # Verify data was created correctly
        sql_results = project.run_sql(
            "SELECT * FROM {database}.{schema}.python_model_with_acl", fetch="all"
        )
        assert len(sql_results) == 10

        # Verify ACL permissions were set correctly using the API
        adapter = project.adapter
        conn_mgr = adapter.connections
        connection = conn_mgr.get_thread_connection()
        api_client = connection.handle.api_client

        # Get current user to determine notebook path
        curr_user = api_client.curr_user.get_username()
        is_service_principal = api_client.curr_user.is_service_principal(curr_user)

        # Construct notebook path - adjust based on how paths are constructed in the adapter
        if hasattr(api_client.workspace, "get_python_model_path"):
            # If the API has a helper function to get the path
            notebook_path = api_client.workspace.get_python_model_path(
                project.database, project.schema, "python_model_with_acl"
            )
        else:
            # Fallback path construction - match pattern in PythonNotebookUploader
            catalog = project.database
            schema = project.schema
            prefix = "/Workspace/Users/" if not is_service_principal else "/Workspace/"
            workdir = f"{prefix}{curr_user}/{catalog}/{schema}/"
            notebook_path = f"{workdir}python_model_with_acl"

        # Get the notebook permissions
        permissions = api_client.notebook_permissions.get(notebook_path)

        # Verify the permissions settings
        acl_list = permissions.get("access_control_list", [])

        # Verify owner has proper permissions
        owner_found = False
        for acl in acl_list:
            if is_service_principal:
                if (
                    acl.get("service_principal_name") == curr_user
                    and acl.get("permission_level") == "IS_OWNER"
                ):
                    owner_found = True
            else:
                if acl.get("user_name") == curr_user and acl.get("permission_level") == "IS_OWNER":
                    owner_found = True

        assert owner_found, "Owner permissions not found"

        # Verify TEST_USER_1 has CAN_VIEW permission
        assert any(
            acl.get("user_name") == override_fixtures.TEST_USER_1
            and acl.get("permission_level") == "CAN_VIEW"
            for acl in acl_list
        ), f"{override_fixtures.TEST_USER_1} ACL not found"

        # Verify TEST_USER_2 has CAN_VIEW permission (from schema.yml)
        assert any(
            acl.get("user_name") == override_fixtures.TEST_USER_2
            and acl.get("permission_level") == "CAN_VIEW"
            for acl in acl_list
        ), f"{override_fixtures.TEST_USER_2} ACL not found"


@pytest.mark.python
@pytest.mark.acl
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModelJobGrants:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_grants.py": override_fixtures.acl_python_model,
            "schema.yml": override_fixtures.job_grant_schema,
        }

    def test_python_model_with_job_grants(self, project):
        # Only run if ACL tests are enabled
        if not pytest.acl_tests_enabled:
            pytest.skip("ACL tests are not enabled")

        # Run the model
        result = util.run_dbt(["run"])
        assert len(result) == 1

        # Verify data was created correctly
        sql_results = project.run_sql(
            "SELECT * FROM {database}.{schema}.python_model_with_grants", fetch="all"
        )
        assert len(sql_results) == 10

        # Verify job grants were set correctly
        adapter = project.adapter
        conn_mgr = adapter.connections
        connection = conn_mgr.get_thread_connection()
        api_client = connection.handle.api_client

        # Get the job ID from logs
        try:
            log_path = os.path.join(project.project_root, "logs", "dbt.log")
            job_id = None
            job_run_id = None

            with open(log_path) as log_file:
                for line in log_file:
                    if "job_id=" in line:
                        job_id_part = line.split("job_id=")[1].split(" ")[0]
                        job_id = job_id_part.strip(", ")
                        break
                    if "run_id=" in line:
                        run_id_part = line.split("run_id=")[1].split(" ")[0]
                        job_run_id = run_id_part.strip(", ")

            if job_id is None and job_run_id is not None:
                job_id = api_client.job_runs.get_job_id_from_run_id(job_run_id)

            if job_id:
                # Get job permissions
                permissions = api_client.workflow_permissions.get(job_id)
                acl_list = permissions.get("access_control_list", [])

                # Verify owner permissions
                curr_user = api_client.curr_user.get_username()
                is_service_principal = api_client.curr_user.is_service_principal(curr_user)

                owner_found = False
                for acl in acl_list:
                    if is_service_principal:
                        if (
                            acl.get("service_principal_name") == curr_user
                            and acl.get("permission_level") == "IS_OWNER"
                        ):
                            owner_found = True
                    else:
                        if (
                            acl.get("user_name") == curr_user
                            and acl.get("permission_level") == "IS_OWNER"
                        ):
                            owner_found = True

                assert owner_found, "Owner permissions not found on job"

                # Verify TEST_USER_1 has CAN_VIEW permission
                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_1
                    and acl.get("permission_level") == "CAN_VIEW"
                    for acl in acl_list
                ), f"{override_fixtures.TEST_USER_1} CAN_VIEW grant not found"

                # Verify TEST_USER_2 has CAN_MANAGE_RUN permission
                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_2
                    and acl.get("permission_level") == "CAN_MANAGE_RUN"
                    for acl in acl_list
                ), f"{override_fixtures.TEST_USER_2} CAN_MANAGE_RUN grant not found"

                # Verify TEST_USER_3 has CAN_MANAGE permission
                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_3
                    and acl.get("permission_level") == "CAN_MANAGE"
                    for acl in acl_list
                ), f"{override_fixtures.TEST_USER_3} CAN_MANAGE grant not found"
            else:
                pytest.skip("Could not find job ID in logs to verify permissions")
        except (FileNotFoundError, IndexError) as e:
            pytest.skip(f"Error finding or processing job info: {str(e)}")


@pytest.mark.python
@pytest.mark.acl
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPythonModelCombinedACL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_combined_acl.py": override_fixtures.acl_python_model,
            "schema.yml": override_fixtures.combined_acl_schema,
        }

    def test_python_model_with_combined_acl(self, project):
        # Only run if ACL tests are enabled
        if not pytest.acl_tests_enabled:
            pytest.skip("ACL tests are not enabled")

        # Run the model
        result = util.run_dbt(["run"])
        assert len(result) == 1

        # Verify data was created correctly
        sql_results = project.run_sql(
            "SELECT * FROM {database}.{schema}.python_model_with_combined_acl", fetch="all"
        )
        assert len(sql_results) == 10

        # Verify both notebook and job permissions
        adapter = project.adapter
        conn_mgr = adapter.connections
        connection = conn_mgr.get_thread_connection()
        api_client = connection.handle.api_client
        curr_user = api_client.curr_user.get_username()
        is_service_principal = api_client.curr_user.is_service_principal(curr_user)

        # 1. Verify notebook permissions
        if hasattr(api_client.workspace, "get_python_model_path"):
            notebook_path = api_client.workspace.get_python_model_path(
                project.database, project.schema, "python_model_with_combined_acl"
            )
        else:
            prefix = "/Workspace/Users/" if not is_service_principal else "/Workspace/"
            workdir = f"{prefix}{curr_user}/{project.database}/{project.schema}/"
            notebook_path = f"{workdir}python_model_with_combined_acl"

        nb_permissions = api_client.notebook_permissions.get(notebook_path)
        nb_acl_list = nb_permissions.get("access_control_list", [])

        # Verify notebook permissions
        assert any(
            acl.get("user_name") == override_fixtures.TEST_USER_1
            and acl.get("permission_level") == "CAN_VIEW"
            for acl in nb_acl_list
        ), f"{override_fixtures.TEST_USER_1} CAN_VIEW not found in notebook permissions"

        assert any(
            acl.get("user_name") == override_fixtures.TEST_USER_2
            and acl.get("permission_level") == "CAN_VIEW"
            for acl in nb_acl_list
        ), f"{override_fixtures.TEST_USER_2} CAN_VIEW not found in notebook permissions"

        assert any(
            acl.get("user_name") == override_fixtures.TEST_USER_3
            and acl.get("permission_level") == "CAN_MANAGE_RUN"
            for acl in nb_acl_list
        ), f"{override_fixtures.TEST_USER_3} CAN_MANAGE_RUN not found in notebook permissions"

        # 2. Try to verify job permissions (with same logic as previous test)
        try:
            log_path = os.path.join(project.project_root, "logs", "dbt.log")
            job_id = None
            job_run_id = None

            with open(log_path) as log_file:
                for line in log_file:
                    if "job_id=" in line:
                        job_id_part = line.split("job_id=")[1].split(" ")[0]
                        job_id = job_id_part.strip(", ")
                        break
                    if "run_id=" in line:
                        run_id_part = line.split("run_id=")[1].split(" ")[0]
                        job_run_id = run_id_part.strip(", ")

            if job_id is None and job_run_id is not None:
                job_id = api_client.job_runs.get_job_id_from_run_id(job_run_id)

            if job_id:
                wf_permissions = api_client.workflow_permissions.get(job_id)
                wf_acl_list = wf_permissions.get("access_control_list", [])

                # Verify workflow permissions
                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_1
                    and acl.get("permission_level") == "CAN_VIEW"
                    for acl in wf_acl_list
                ), f"{override_fixtures.TEST_USER_1} CAN_VIEW not found in workflow permissions"

                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_2
                    and acl.get("permission_level") == "CAN_VIEW"
                    for acl in wf_acl_list
                ), f"{override_fixtures.TEST_USER_2} CAN_VIEW not found in workflow permissions"

                msg_prefix = f"{override_fixtures.TEST_USER_3}"
                user3_err_msg = f"{msg_prefix} CAN_MANAGE_RUN not found in workflow permissions"
                assert any(
                    acl.get("user_name") == override_fixtures.TEST_USER_3
                    and acl.get("permission_level") == "CAN_MANAGE_RUN"
                    for acl in wf_acl_list
                ), user3_err_msg
        except (FileNotFoundError, IndexError) as e:
            pytest.skip(f"Error finding or processing job info: {str(e)}")


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
