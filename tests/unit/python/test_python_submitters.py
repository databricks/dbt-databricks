from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.python_models.python_config import ParsedPythonModel, PythonModelConfig
from dbt.adapters.databricks.python_models.python_submissions import (
    PythonCommandSubmitter,
    PythonJobConfigCompiler,
    PythonJobDetails,
    PythonNotebookSubmitter,
    PythonNotebookUploader,
    PythonNotebookWorkflowSubmitter,
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


@pytest.fixture
def parsed_model():
    return ParsedPythonModel(
        catalog="hive_metastore", schema="default", identifier="alias", config=PythonModelConfig()
    )


class TestPythonCommandSubmitter:
    @pytest.fixture
    def cluster_id(self):
        return "cluster_id"

    @pytest.fixture
    def submitter(self, client, tracker, cluster_id, context_id, parsed_model):
        client.command_contexts.create.return_value = context_id
        return PythonCommandSubmitter(client, tracker, cluster_id, parsed_model)

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

    def test_submit__with_packages(self, client, tracker, cluster_id, context_id, compiled_code):
        client.command_contexts.create.return_value = context_id
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas", "numpy==1.24.0", "scikit-learn>=1.0"],
            notebook_scoped_libraries=True,
        )
        submitter = PythonCommandSubmitter(client, tracker, cluster_id, parsed_model)

        command_exec = client.commands.execute.return_value
        submitter.submit(compiled_code)

        # Verify the code includes the pip install command
        expected_code = [
            "%pip install  -q pandas numpy==1.24.0 scikit-learn>=1.0",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        client.commands.execute.assert_called_once_with(cluster_id, context_id, expected_code)
        client.commands.poll_for_completion.assert_called_once_with(command_exec)
        tracker.remove_command.assert_called_once_with(command_exec)

    def test_submit__with_packages_and_index_url(
        self, client, tracker, cluster_id, context_id, compiled_code
    ):
        client.command_contexts.create.return_value = context_id
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas"],
            notebook_scoped_libraries=True,
            index_url="https://example.com/pypi/simple",
        )
        submitter = PythonCommandSubmitter(client, tracker, cluster_id, parsed_model)

        command_exec = client.commands.execute.return_value
        submitter.submit(compiled_code)

        # Verify the code includes the pip install command
        expected_code = [
            "%pip install --index-url https://example.com/pypi/simple -q pandas",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        client.commands.execute.assert_called_once_with(cluster_id, context_id, expected_code)
        client.commands.poll_for_completion.assert_called_once_with(command_exec)
        tracker.remove_command.assert_called_once_with(command_exec)

    def test_submit__with_empty_packages(
        self, client, tracker, cluster_id, context_id, compiled_code
    ):
        client.command_contexts.create.return_value = context_id
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(packages=[], notebook_scoped_libraries=True)
        submitter = PythonCommandSubmitter(client, tracker, cluster_id, parsed_model)

        command_exec = client.commands.execute.return_value
        submitter.submit(compiled_code)

        # Verify the code is unchanged
        client.commands.execute.assert_called_once_with(cluster_id, context_id, compiled_code)
        client.commands.poll_for_completion.assert_called_once_with(command_exec)


class TestPythonNotebookSubmitter:
    @pytest.fixture
    def submitter(self, client, tracker, uploader, config_compiler, parsed_model):
        return PythonNotebookSubmitter(client, tracker, uploader, config_compiler, parsed_model)

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

    def test_submit__with_packages(self, client, tracker, uploader, config_compiler, compiled_code):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas", "numpy==1.24.0", "scikit-learn>=1.0"],
            notebook_scoped_libraries=True,
        )
        submitter = PythonNotebookSubmitter(
            client, tracker, uploader, config_compiler, parsed_model
        )

        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        config_compiler.compile.return_value = job_config
        uploader.upload.return_value = "upload_path"

        submitter.submit(compiled_code)

        # Verify the uploader was called with the modified code
        expected_code = [
            "%pip install  -q pandas numpy==1.24.0 scikit-learn>=1.0",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        uploader.upload.assert_called_once_with(expected_code)

    def test_submit__with_packages_and_index_url(
        self, client, tracker, uploader, config_compiler, compiled_code
    ):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas"],
            notebook_scoped_libraries=True,
            index_url="https://example.com/pypi/simple",
        )
        submitter = PythonNotebookSubmitter(
            client, tracker, uploader, config_compiler, parsed_model
        )

        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        config_compiler.compile.return_value = job_config
        uploader.upload.return_value = "upload_path"

        submitter.submit(compiled_code)

        # Verify the uploader was called with the modified code
        expected_code = [
            "%pip install --index-url https://example.com/pypi/simple -q pandas",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        uploader.upload.assert_called_once_with(expected_code)

    def test_submit__with_empty_packages(
        self, client, tracker, uploader, config_compiler, compiled_code
    ):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(packages=[], notebook_scoped_libraries=True)
        submitter = PythonNotebookSubmitter(
            client, tracker, uploader, config_compiler, parsed_model
        )

        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        config_compiler.compile.return_value = job_config
        uploader.upload.return_value = "upload_path"

        submitter.submit(compiled_code)

        # Verify the code is unchanged
        uploader.upload.assert_called_once_with(compiled_code)

    def test_submit__with_packages_not_notebook_scoped(
        self, client, tracker, uploader, config_compiler, compiled_code
    ):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas"], notebook_scoped_libraries=False
        )
        submitter = PythonNotebookSubmitter(
            client, tracker, uploader, config_compiler, parsed_model
        )

        job_config = Mock()
        job_config.job_spec = {}
        job_config.additional_job_config = {}
        config_compiler.compile.return_value = job_config
        uploader.upload.return_value = "upload_path"

        submitter.submit(compiled_code)

        # Verify the code is unchanged when notebook_scoped_libraries is False
        uploader.upload.assert_called_once_with(compiled_code)


class TestPythonNotebookWorkflowSubmitter:
    @pytest.fixture
    def permission_builder(self):
        return Mock()

    @pytest.fixture
    def workflow_creater(self):
        return Mock()

    @pytest.fixture
    def submitter(
        self,
        client,
        tracker,
        uploader,
        config_compiler,
        permission_builder,
        workflow_creater,
        parsed_model,
    ):
        return PythonNotebookWorkflowSubmitter(
            client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            {},
            [],
            parsed_model,
        )

    def test_submit__golden_path(self, submitter, compiled_code):
        submitter.uploader.upload.return_value = "upload_path"
        submitter.config_compiler.compile.return_value = ({}, "existing_job_id")
        submitter.workflow_creater.create_or_update.return_value = "existing_job_id"
        submitter.permission_builder.build_job_permissions.return_value = []
        submitter.api_client.workflows.run.return_value = "run_id"
        submitter.submit(compiled_code)
        submitter.tracker.insert_run_id.assert_called_once_with("run_id")
        submitter.api_client.job_runs.poll_for_completion.assert_called_once_with("run_id")
        submitter.tracker.remove_run_id.assert_called_once_with("run_id")

    def test_submit__poll_fails__cleans_up(self, submitter, compiled_code):
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
        parsed_model.config.access_control_list = []
        parsed_model.config.python_job_config.additional_task_settings = {}
        parsed_model.config.python_job_config.dict.return_value = {}
        submitter = PythonNotebookWorkflowSubmitter.create(client, tracker, parsed_model)
        assert submitter.api_client == client
        assert submitter.tracker == tracker
        assert isinstance(submitter.uploader, PythonNotebookUploader)
        assert isinstance(submitter.config_compiler, PythonWorkflowConfigCompiler)
        assert isinstance(submitter.workflow_creater, PythonWorkflowCreator)

    def test_submit__with_packages(self, client, tracker, uploader, compiled_code):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas", "numpy==1.24.0", "scikit-learn>=1.0"],
            notebook_scoped_libraries=True,
        )
        parsed_model.config.python_job_config.grants = {}
        parsed_model.config.access_control_list = []

        config_compiler = Mock()
        permission_builder = Mock()
        workflow_creater = Mock()

        submitter = PythonNotebookWorkflowSubmitter(
            client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            {},
            [],
            parsed_model,
        )

        uploader.upload.return_value = "upload_path"
        config_compiler.compile.return_value = ({}, "existing_job_id")
        workflow_creater.create_or_update.return_value = "existing_job_id"
        permission_builder.build_job_permissions.return_value = []
        client.workflows.run.return_value = "run_id"

        submitter.submit(compiled_code)

        # Verify the uploader was called with the modified code
        expected_code = [
            "%pip install  -q pandas numpy==1.24.0 scikit-learn>=1.0",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        uploader.upload.assert_called_once_with(expected_code)

    def test_submit__with_packages_and_index_url(self, client, tracker, uploader, compiled_code):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas"],
            notebook_scoped_libraries=True,
            index_url="https://example.com/pypi/simple",
        )
        parsed_model.config.python_job_config.grants = {}
        parsed_model.config.access_control_list = []

        config_compiler = Mock()
        permission_builder = Mock()
        workflow_creater = Mock()

        submitter = PythonNotebookWorkflowSubmitter(
            client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            {},
            [],
            parsed_model,
        )

        uploader.upload.return_value = "upload_path"
        config_compiler.compile.return_value = ({}, "existing_job_id")
        workflow_creater.create_or_update.return_value = "existing_job_id"
        permission_builder.build_job_permissions.return_value = []
        client.workflows.run.return_value = "run_id"

        submitter.submit(compiled_code)

        # Verify the uploader was called with the modified code
        expected_code = [
            "%pip install --index-url https://example.com/pypi/simple -q pandas",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        expected_code = "\n\n# COMMAND ----------\n\n".join(expected_code)
        uploader.upload.assert_called_once_with(expected_code)

    def test_submit__with_empty_packages(self, client, tracker, uploader, compiled_code):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(packages=[], notebook_scoped_libraries=True)
        parsed_model.config.python_job_config.grants = {}
        parsed_model.config.access_control_list = []

        config_compiler = Mock()
        permission_builder = Mock()
        workflow_creater = Mock()

        submitter = PythonNotebookWorkflowSubmitter(
            client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            {},
            [],
            parsed_model,
        )

        uploader.upload.return_value = "upload_path"
        config_compiler.compile.return_value = ({}, "existing_job_id")
        workflow_creater.create_or_update.return_value = "existing_job_id"
        permission_builder.build_job_permissions.return_value = []
        client.workflows.run.return_value = "run_id"

        submitter.submit(compiled_code)

        # Verify the code is unchanged
        uploader.upload.assert_called_once_with(compiled_code)

    def test_submit__with_packages_not_notebook_scoped(
        self, client, tracker, uploader, compiled_code
    ):
        parsed_model = Mock()
        parsed_model.config = PythonModelConfig(
            packages=["pandas"], notebook_scoped_libraries=False
        )
        parsed_model.config.python_job_config.grants = {}
        parsed_model.config.access_control_list = []

        config_compiler = Mock()
        permission_builder = Mock()
        workflow_creater = Mock()

        submitter = PythonNotebookWorkflowSubmitter(
            client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            {},
            [],
            parsed_model,
        )

        uploader.upload.return_value = "upload_path"
        config_compiler.compile.return_value = ({}, "existing_job_id")
        workflow_creater.create_or_update.return_value = "existing_job_id"
        permission_builder.build_job_permissions.return_value = []
        client.workflows.run.return_value = "run_id"

        submitter.submit(compiled_code)

        # Verify the code is unchanged when notebook_scoped_libraries is False
        uploader.upload.assert_called_once_with(compiled_code)
