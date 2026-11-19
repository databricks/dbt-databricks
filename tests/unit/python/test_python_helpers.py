from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.python_submissions import (
    AllPurposeClusterPythonJobHelper,
    JobClusterPythonJobHelper,
    PythonCommandSubmitter,
    PythonNotebookSubmitter,
    PythonNotebookWorkflowSubmitter,
    ServerlessClusterPythonJobHelper,
    WorkflowPythonJobHelper,
)


@pytest.fixture
def parsed_model():
    return {"alias": "test", "config": {}}


@pytest.fixture
def credentials():
    c = Mock()
    c.get_all_http_headers.return_value = {}
    c.connection_parameters = {}
    c.http_path = "/sql/1.0/warehouses/abc"
    return c


class TestJobClusterPythonJobHelper:
    def test_init__golden_path(self, parsed_model, credentials):
        parsed_model["config"]["job_cluster_config"] = {"cluster_id": "test"}
        helper = JobClusterPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookSubmitter)
        assert helper.command_submitter.config_compiler.cluster_spec == {
            "libraries": [],
            "new_cluster": {"cluster_id": "test"},
        }

    def test_init__no_cluster_config(self, parsed_model, credentials):
        with pytest.raises(ValueError) as exc:
            JobClusterPythonJobHelper(parsed_model, credentials)
        assert exc.match(
            "`job_cluster_config` is required for the `job_cluster` submission method."
        )


class TestAllPurposeClusterPythonJobHelper:
    def test_init__no_notebook_credential_http_path(self, parsed_model, credentials):
        credentials.extract_cluster_id.return_value = "test"
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonCommandSubmitter)
        assert helper.cluster_id == "test"

    def test_init__notebook_cluster_id(self, parsed_model, credentials):
        parsed_model["config"] = {"create_notebook": True, "cluster_id": "test"}
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookSubmitter)
        assert helper.cluster_id == "test"

    def test_init__no_cluster_id(self, parsed_model, credentials):
        credentials.extract_cluster_id.return_value = None
        with pytest.raises(ValueError) as exc:
            AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert exc.match(
            "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required "
            "for the `all_purpose_cluster` submission method."
        )


class TestAllPurposeClusterPythonJobHelperDatabricksCompute:
    @pytest.fixture
    def credentials(self, credentials):
        credentials.http_path = "sql/protocolv1/o/1234567890/default-cluster"
        credentials.extract_cluster_id.side_effect = DatabricksCredentials.extract_cluster_id
        credentials.compute = {
            "alternate_cluster": {"http_path": "sql/protocolv1/o/1234567890/alternate-cluster"},
            "alternate_warehouse": {"http_path": "/sql/1.0/warehouses/abc"},
        }
        return credentials

    def test_init__databricks_compute_cluster(self, parsed_model, credentials):
        parsed_model["config"] = {"databricks_compute": "alternate_cluster"}
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert helper.cluster_id == "alternate-cluster"

    def test_init__databricks_compute_warehouse_uses_default_cluster(
        self, parsed_model, credentials
    ):
        parsed_model["config"] = {"databricks_compute": "alternate_warehouse"}
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert helper.cluster_id == "default-cluster"

    def test_init__cluster_id_overrides_databricks_compute(self, parsed_model, credentials):
        parsed_model["config"] = {"databricks_compute": "alternate_cluster", "cluster_id": "test"}
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert helper.cluster_id == "test"

    def test_init__http_path_overrides_databricks_compute(self, parsed_model, credentials):
        parsed_model["config"] = {
            "databricks_compute": "alternate_cluster",
            "http_path": "sql/protocolv1/o/1234567890/http-path-cluster",
        }
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert helper.cluster_id == "http-path-cluster"

    def test_init__unknown_databricks_compute(self, parsed_model, credentials):
        parsed_model["config"] = {"databricks_compute": "missing"}
        with pytest.raises(DbtRuntimeError) as exc:
            AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        assert exc.match("Compute resource missing does not exist or does not specify http_path")


class TestServerlessClusterPythonJobHelper:
    def test_build_submitter(self, parsed_model, credentials):
        helper = ServerlessClusterPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookSubmitter)
        assert helper.command_submitter.config_compiler.cluster_spec == {"libraries": []}


class TestWorkflowPythonJobHelper:
    def test_init__golden_path(self, parsed_model, credentials):
        helper = WorkflowPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookWorkflowSubmitter)
