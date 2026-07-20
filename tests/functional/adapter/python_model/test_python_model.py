import json
import os

import pytest
from dbt.tests import util
from dbt.tests.adapter.python_model import test_python_model as fixtures
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)

from tests.functional.adapter.fixtures import ManagedIcebergMixin, MaterializationV2Mixin
from tests.functional.adapter.iceberg.test_iceberg_support import get_provider
from tests.functional.adapter.python_model import fixtures as override_fixtures

# Check if ACL tests should be enabled
# Set DBT_ENABLE_ACL_TESTS=1 to enable ACL tests
# Users for tests are set via DBT_TEST_USER_1/2/3 environment variables
ACL_TESTS_ENABLED = os.environ.get("DBT_ENABLE_ACL_TESTS") == "1"


def verify_temp_tables_cleaned(project):
    verify_temp_table_cleaned(project, "*__dbt_stg")
    verify_temp_table_cleaned(project, "*__dbt_tmp")


def verify_temp_table_cleaned(project, suffix):
    tmp_tables = project.run_sql(
        "SHOW TABLES IN {database}.{schema} LIKE '" + f"{suffix}'", fetch="all"
    )
    assert len(tmp_tables) == 0


class SchemaNameVarMixin:
    """Make the schema-change tests resilient to dbt-core test-isolation leakage.

    These classes don't inherit ``BasePythonModelTests`` and run plain ``dbt run``. On
    dbt-core 1.11.2 a sibling class's ``test_source`` schema.yml -- whose ``schema``
    renders ``var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE'))`` -- can bleed into these
    classes' parse when they run after one in the same xdist worker (``--dist=loadfile``),
    failing with ``EnvVarMissingError``. Rendering that source needs both the env var
    (read from the invocation context) and the ``test_run_schema`` var. Schema-yaml
    rendering resolves ``var()`` from CLI vars only, so the var must be passed via
    ``--vars`` -- exactly as dbt-core's ``BasePythonModelTests`` does -- not via
    project-level ``vars``.
    """

    @pytest.fixture(scope="class", autouse=True)
    def schema_name_env_var(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"
        yield
        os.environ.pop("DBT_TEST_SCHEMA_NAME_VARIABLE", None)

    @staticmethod
    def schema_name_vars(project):
        return ["--vars", json.dumps({"test_run_schema": project.test_schema})]


class PythonModelDataMixin:
    """Assert that the built ``my_python_model`` holds the expected rows.

    ``basic_python`` refs ``my_sql_model`` (six ``id=1`` rows) and returns
    ``df.limit(2)``, so whatever compute path the host class exercises, the
    resulting table must hold exactly those two ``id=1`` rows.
    """

    def test_python_model_data(self, project):
        run_vars = ["--vars", json.dumps({"test_run_schema": project.test_schema})]
        util.run_dbt(["seed", *run_vars])
        util.run_dbt(["run", *run_vars])
        rows = project.run_sql("SELECT id FROM {database}.{schema}.my_python_model", fetch="all")
        assert sorted(row[0] for row in rows) == [1, 1]


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonModel(BasePythonModelTests):
    """Default Python model tests using serverless compute for speed and reliability."""

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
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonFailureModel:
    """Test Python model error handling using serverless compute."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_failure_model.py": override_fixtures.python_error_model}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_failure_model(self, project):
        util.run_dbt(["run"], expect_pass=False)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonFailureModelNotebook(TestPythonFailureModel):
    """Test Python model error handling with notebooks using serverless compute."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonIncrementalModel(BasePythonIncrementalTests):
    """Test Python incremental models using serverless compute."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+submission_method": "serverless_cluster"}}


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestChangingSchema(SchemaNameVarMixin):
    """Test Python model schema changes using serverless compute."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": override_fixtures.simple_python_model}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_changing_schema(self, project):
        schema_vars = self.schema_name_vars(project)
        util.run_dbt(["run", *schema_vars])
        util.write_file(
            override_fixtures.simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        util.run_dbt(["run", *schema_vars])
        columns = project.run_sql(
            "SELECT column_name FROM {database}.information_schema.columns "
            "WHERE table_schema = '{schema}' AND table_name = 'simple_python_model' "
            "ORDER BY ordinal_position",
            fetch="all",
        )
        assert [c[0] for c in columns] == ["test1", "test3"]


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestChangingSchemaIncremental(SchemaNameVarMixin):
    """Test Python incremental schema changes using serverless compute."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.py": override_fixtures.incremental_model}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_incremental.csv": override_fixtures.expected_incremental}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_changing_schema_via_incremental(self, project):
        schema_vars = self.schema_name_vars(project)
        util.run_dbt(["seed", *schema_vars])
        util.run_dbt(["run", *schema_vars])
        util.run_dbt(["run", *schema_vars])

        util.check_relations_equal(project.adapter, ["incremental_model", "expected_incremental"])


@pytest.mark.python
@pytest.mark.skip_kernel  # pins model http_path to a cluster; SEA/kernel is warehouse-only
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
@pytest.mark.flaky(reruns=2, reruns_delay=120)
class TestSpecifyingHttpPath(PythonModelDataMixin, BasePythonModelTests):
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
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestJobCluster(PythonModelDataMixin, BasePythonModelTests):
    """Test Python models using job_cluster submission method with ephemeral clusters."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.job_cluster_schema,
            "my_sql_model.sql": fixtures.basic_sql,
            "my_versioned_sql_model_v1.sql": fixtures.basic_sql,
            "my_python_model.py": fixtures.basic_python,
            "second_sql_model.sql": fixtures.second_sql,
        }


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestServerlessCluster(PythonModelDataMixin, BasePythonModelTests):
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
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
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
                "+submission_method": "serverless_cluster",
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
                "+submission_method": "serverless_cluster",
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestWorkflowJob:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.workflow_schema,
            "my_workflow_model.py": override_fixtures.workflow_python_model,
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
@pytest.mark.skipif(
    not ACL_TESTS_ENABLED, reason="ACL tests disabled (set DBT_ENABLE_ACL_TESTS=1 to enable)"
)
class TestPythonModelNotebookACL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_notebook_acl.py": override_fixtures.simple_python_model,
            "schema.yml": override_fixtures.notebook_acl_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_python_model_with_notebook_acl(self, project):
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


@pytest.mark.python
@pytest.mark.acl
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
@pytest.mark.skipif(
    not ACL_TESTS_ENABLED, reason="ACL tests disabled (set DBT_ENABLE_ACL_TESTS=1 to enable)"
)
class TestPythonModelAccessControlList:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_acl.py": override_fixtures.simple_python_model,
            "schema.yml": override_fixtures.access_control_list_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_python_model_with_access_control_list(self, project):
        adapter = project.adapter
        conn_mgr = adapter.connections
        api_client = conn_mgr.api_client
        curr_user = api_client.curr_user.get_username()

        username = curr_user
        notebook_path = (
            f"/Users/{username}/dbt_python_models/"
            f"{project.database}/{project.test_schema}/python_model_with_acl"
        )

        try:
            result = util.run_dbt(["run"])
            assert len(result) == 1

            sql_results = project.run_sql(
                "SELECT * FROM {database}.{schema}.python_model_with_acl", fetch="all"
            )
            assert len(sql_results) == 10

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


@pytest.mark.skip_profile("databricks_cluster")
class TestChangingSchemaV2(SchemaNameVarMixin, MaterializationV2Mixin):
    """Test Python model schema changes with V2 materialization using serverless compute."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": override_fixtures.simple_python_model}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_changing_unique_tmp_table_suffix(self, project):
        schema_vars = self.schema_name_vars(project)
        util.run_dbt(["run", *schema_vars])
        util.write_file(
            override_fixtures.simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        util.run_dbt(["run", *schema_vars])
        verify_temp_tables_cleaned(project)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestChangingSchemaIncrementalV2(SchemaNameVarMixin, MaterializationV2Mixin):
    """Test Python incremental schema changes with V2 materialization using serverless compute."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.py": override_fixtures.simple_incremental_python_model}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

    def test_changing_unique_tmp_table_suffix(self, project):
        schema_vars = self.schema_name_vars(project)
        util.run_dbt(["run", *schema_vars])
        util.write_file(
            override_fixtures.simple_incremental_python_model_v2,
            project.project_root + "/models",
            "incremental_model.py",
        )
        util.run_dbt(["run", *schema_vars])
        verify_temp_tables_cleaned(project)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestNotebookScopedPackagesCommandAPI:
    """Test notebook_scoped_libraries=True with the Command API path (create_notebook=False).

    Verifies that packages are installed via %pip install in the notebook code
    rather than as cluster-level libraries, and the model runs successfully.
    """

    @pytest.fixture(scope="class")
    def models(self):
        model = override_fixtures.notebook_scoped_packages_cmd_api_model
        return {"notebook_scoped_packages_cmd.py": model}

    def test_notebook_scoped_packages_command_api(self, project):
        results = util.run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            "SELECT * FROM {database}.{schema}.notebook_scoped_packages_cmd ORDER BY id",
            fetch="all",
        )
        assert len(rows) == 2


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestNotebookScopedPackagesNotebookRun:
    """Test notebook_scoped_libraries=True with the notebook job run path (create_notebook=True).

    Verifies that packages are installed via %pip install in the notebook code
    when using the notebook job submission path.
    """

    @pytest.fixture(scope="class")
    def models(self):
        model = override_fixtures.notebook_scoped_packages_notebook_run_model
        return {"notebook_scoped_packages_nb.py": model}

    def test_notebook_scoped_packages_notebook_run(self, project):
        results = util.run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            "SELECT * FROM {database}.{schema}.notebook_scoped_packages_nb ORDER BY id",
            fetch="all",
        )
        assert len(rows) == 2


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestAllPurposeClusterCommandAPI(PythonModelDataMixin, BasePythonModelTests):
    """Test Python models using all_purpose_cluster with Command API (create_notebook=False).

    This tests the command execution path that uses the Command API directly
    without creating notebooks, which exercises the timeout fix for command execution.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.all_purpose_command_api_schema,
            "my_sql_model.sql": fixtures.basic_sql,
            "my_versioned_sql_model_v1.sql": fixtures.basic_sql,
            "my_python_model.py": fixtures.basic_python,
            "second_sql_model.sql": fixtures.second_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+submission_method": "all_purpose_cluster",
                "+create_notebook": False,  # Use Command API, not notebook submission
            }
        }


@pytest.mark.python
class TestJobClusterMissingConfig(SchemaNameVarMixin):
    """job_cluster submission requires job_cluster_config; omitting it fails the run."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"jc_no_config.py": override_fixtures.job_cluster_missing_config_model}

    def test_missing_job_cluster_config_fails(self, project):
        util.run_dbt(["run", *self.schema_name_vars(project)], expect_pass=False)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestAllPurposeClusterMissingClusterId(SchemaNameVarMixin):
    """all_purpose_cluster needs a resolvable cluster_id; on a SQL warehouse connection
    neither http_path nor cluster_id resolves to one, so the run fails."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"ap_no_cluster.py": override_fixtures.all_purpose_missing_cluster_model}

    def test_missing_cluster_id_fails(self, project):
        util.run_dbt(["run", *self.schema_name_vars(project)], expect_pass=False)


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestManagedIcebergPythonModel(ManagedIcebergMixin):
    """Regression for #1591: managed Iceberg Python models must materialize as Iceberg."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"managed_iceberg_python.py": override_fixtures.managed_iceberg_python_model}

    def test_managed_iceberg_python_model_materializes(self, project):
        util.run_dbt(["run"])
        assert get_provider(project, "managed_iceberg_python") == "iceberg"
        rows = project.run_sql(
            "SELECT id FROM {database}.{schema}.managed_iceberg_python ORDER BY id",
            fetch="all",
        )
        assert [row[0] for row in rows] == [1]
