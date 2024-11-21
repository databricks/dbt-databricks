from unittest.mock import Mock

import pytest

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


class TestServerlessClusterPythonJobHelper:
    def test_build_submitter(self, parsed_model, credentials):
        helper = ServerlessClusterPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookSubmitter)
        assert helper.command_submitter.config_compiler.cluster_spec == {"libraries": []}


class TestWorkflowPythonJobHelper:
    def test_init__golden_path(self, parsed_model, credentials):
        helper = WorkflowPythonJobHelper(parsed_model, credentials)
        assert isinstance(helper.command_submitter, PythonNotebookWorkflowSubmitter)
