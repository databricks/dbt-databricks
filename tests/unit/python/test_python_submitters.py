from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.python_models.python_submissions import (
    PythonCommandSubmitter,
    PythonJobConfigCompiler,
    PythonJobDetails,
    PythonNotebookSubmitter,
    PythonNotebookUploader,
    PythonNotebookWorkflowSubmitter,
    PythonPermissionBuilder,
    PythonWorkflowConfigCompiler,
    PythonWorkflowCreator,
)


@pytest.fixture
def client():
    return Mock()


@pytest.fixture
def tracker():
    return Mock()


@pytest.fixture
def compiled_code():
    return "compiled_code"


@pytest.fixture
def config_compiler():
    compiler = Mock()
    compiler.compile.return_value = PythonJobDetails(
        run_name="name", job_spec={}, additional_job_config={}
    )
    return compiler


@pytest.fixture
def uploader():
    return Mock()


class TestPythonCommandSubmitter:
    @pytest.fixture
    def cluster_id(self):
        return "cluster_id"

    @pytest.fixture
    def submitter(self, client, tracker, cluster_id, context_id):
        client.command_contexts.create.return_value = context_id
        return PythonCommandSubmitter(client, tracker, cluster_id)

    @pytest.fixture
    def context_id(self):
        return "context_id"

    def test_submit__golden_path(
        self, submitter, compiled_code, client, cluster_id, context_id, tracker
    ):
        command_exec = client.commands.execute.return_value
        submitter.submit(compiled_code)
        client.commands.execute.assert_called_once_with(cluster_id, context_id, compiled_code)
        client.commands.poll_for_completion.assert_called_once_with(command_exec)
        client.command_contexts.destroy.assert_called_once_with(cluster_id, context_id)
        tracker.remove_command.assert_called_once_with(command_exec)

    def test_submit__execute_fails__cleans_up(
        self, submitter, compiled_code, client, cluster_id, context_id, tracker
    ):
        client.commands.execute.side_effect = Exception("error")
        with pytest.raises(Exception):
            submitter.submit(compiled_code)
        client.command_contexts.destroy.assert_called_once_with(cluster_id, context_id)
        tracker.remove_command.assert_not_called()

    def test_submit__poll_fails__cleans_up(
        self, submitter, compiled_code, client, cluster_id, context_id, tracker
    ):
        command_exec = client.commands.execute.return_value
        client.commands.poll_for_completion.side_effect = Exception("error")
        with pytest.raises(Exception):
            submitter.submit(compiled_code)
        client.command_contexts.destroy.assert_called_once_with(cluster_id, context_id)
        tracker.remove_command.assert_called_once_with(command_exec)


class TestPythonNotebookSubmitter:
    @pytest.fixture
    def submitter(self, client, tracker, uploader, config_compiler):
        return PythonNotebookSubmitter(client, tracker, uploader, config_compiler)

    @pytest.fixture
    def run_id(self, client):
        return client.job_runs.submit.return_value

    def test_submit__golden_path(self, submitter, compiled_code, client, tracker, run_id):
        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        submitter.config_compiler.compile.return_value = job_config

        submitter.submit(compiled_code)

        tracker.insert_run_id.assert_called_once_with(run_id)
        client.job_runs.poll_for_completion.assert_called_once_with(run_id)
        tracker.remove_run_id.assert_called_once_with(run_id)
        client.workflow_permissions.put.assert_not_called()

    def test_submit__with_acls(self, submitter, compiled_code, client, tracker, run_id):
        job_config = Mock()
        job_config.job_spec = {
            "access_control_list": [{"user_name": "user", "permission_level": "CAN_RUN"}]
        }
        job_config.additional_job_config = {}
        submitter.config_compiler.compile.return_value = job_config

        client.job_runs.get_job_id_from_run_id.return_value = "job_id"

        submitter.submit(compiled_code)

        tracker.insert_run_id.assert_called_once_with(run_id)
        client.job_runs.get_job_id_from_run_id.assert_called_once_with(run_id)
        client.workflow_permissions.patch.assert_called_once_with(
            "job_id", [{"user_name": "user", "permission_level": "CAN_RUN"}]
        )
        client.job_runs.poll_for_completion.assert_called_once_with(run_id)
        tracker.remove_run_id.assert_called_once_with(run_id)

    def test_submit__with_acls_permission_error(
        self, submitter, compiled_code, client, tracker, run_id
    ):
        job_config = Mock()
        job_config.job_spec = {
            "access_control_list": [{"user_name": "user", "permission_level": "CAN_RUN"}]
        }
        job_config.additional_job_config = {}
        submitter.config_compiler.compile.return_value = job_config

        client.job_runs.get_job_id_from_run_id.side_effect = Exception("Error getting job_id")

        with pytest.raises(DbtRuntimeError):
            submitter.submit(compiled_code)

        tracker.insert_run_id.assert_called_once_with(run_id)
        client.job_runs.get_job_id_from_run_id.assert_called_once_with(run_id)
        client.workflow_permissions.put.assert_not_called()
        tracker.remove_run_id.assert_called_once_with(run_id)

    def test_submit__poll_fails__cleans_up(self, submitter, compiled_code, client, tracker, run_id):
        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        submitter.config_compiler.compile.return_value = job_config

        client.job_runs.poll_for_completion.side_effect = Exception("error")
        with pytest.raises(Exception):
            submitter.submit(compiled_code)
        tracker.remove_run_id.assert_called_once_with(run_id)

    def test_create__golden_path(self, client, tracker):
        parsed_model = Mock()
        parsed_model.config.packages = []
        parsed_model.config.additional_libs = []
        cluster_spec = {}
        submitter = PythonNotebookSubmitter.create(client, tracker, parsed_model, cluster_spec)
        assert submitter.api_client == client
        assert submitter.tracker == tracker
        assert isinstance(submitter.uploader, PythonNotebookUploader)
        assert isinstance(submitter.config_compiler, PythonJobConfigCompiler)


class TestPythonNotebookWorkflowSubmitter:
    @pytest.fixture
    def permission_builder(self):
        return Mock()

    @pytest.fixture
    def workflow_creater(self):
        return Mock()

    @pytest.fixture
    def submitter(
        self, client, tracker, uploader, config_compiler, permission_builder, workflow_creater
    ):
        return PythonNotebookWorkflowSubmitter(
            client, tracker, uploader, config_compiler, permission_builder, workflow_creater, {}
        )

    def test_submit__golden_path(self, submitter):
        submitter.uploader.upload.return_value = "upload_path"
        submitter.config_compiler.compile.return_value = ({}, "existing_job_id")
        submitter.workflow_creater.create_or_update.return_value = "existing_job_id"
        submitter.permission_builder.build_job_permissions.return_value = []
        submitter.api_client.workflows.run.return_value = "run_id"
        submitter.submit(compiled_code)
        submitter.tracker.insert_run_id.assert_called_once_with("run_id")
        submitter.api_client.job_runs.poll_for_completion.assert_called_once_with("run_id")
        submitter.tracker.remove_run_id.assert_called_once_with("run_id")

    def test_submit__poll_fails__cleans_up(self, submitter):
        submitter.uploader.upload.return_value = "upload_path"
        submitter.config_compiler.compile.return_value = ({}, "existing_job_id")
        submitter.workflow_creater.create_or_update.return_value = "existing_job_id"
        submitter.permission_builder.build_job_permissions.return_value = []
        submitter.api_client.workflows.run.return_value = "run_id"
        submitter.api_client.job_runs.poll_for_completion.side_effect = Exception("error")
        with pytest.raises(Exception):
            submitter.submit(compiled_code)
        submitter.tracker.remove_run_id.assert_called_once_with("run_id")

    def test_create__golden_path(self, client, tracker):
        parsed_model = Mock()
        parsed_model.config.python_job_config.grants = {}
        parsed_model.config.python_job_config.additional_task_settings = {}
        parsed_model.config.python_job_config.dict.return_value = {}
        submitter = PythonNotebookWorkflowSubmitter.create(client, tracker, parsed_model)
        assert submitter.api_client == client
        assert submitter.tracker == tracker
        assert isinstance(submitter.uploader, PythonNotebookUploader)
        assert isinstance(submitter.config_compiler, PythonWorkflowConfigCompiler)
        assert isinstance(submitter.permission_builder, PythonPermissionBuilder)
        assert isinstance(submitter.workflow_creater, PythonWorkflowCreator)
