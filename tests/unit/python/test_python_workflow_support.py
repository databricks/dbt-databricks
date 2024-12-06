from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.python_models.python_submissions import (
    PythonWorkflowConfigCompiler,
    PythonWorkflowCreator,
)


class TestPythonWorkflowConfigCompiler:
    @pytest.fixture
    def parsed_model(self):
        model = Mock()
        model.catalog = "catalog"
        model.schema_ = "schema"
        model.identifier = "identifier"
        return model

    def test_workflow_name__no_config(self, parsed_model):
        parsed_model.config.python_job_config = None
        assert (
            PythonWorkflowConfigCompiler.workflow_name(parsed_model)
            == "dbt__catalog-schema-identifier"
        )

    def test_workflow_name__config_without_name(self, parsed_model):
        parsed_model.config.python_job_config = {}
        assert (
            PythonWorkflowConfigCompiler.workflow_name(parsed_model)
            == "dbt__catalog-schema-identifier"
        )

    def test_workflow_name__config_with_name(self, parsed_model):
        parsed_model.config.python_job_config.name = "test"
        assert PythonWorkflowConfigCompiler.workflow_name(parsed_model) == "test"

    def test_cluster_settings__no_cluster_id(self, parsed_model):
        parsed_model.config.job_cluster_config = None
        parsed_model.config.cluster_id = None
        assert PythonWorkflowConfigCompiler.cluster_settings(parsed_model) == {}

    def test_cluster_settings__no_job_cluster_config(self, parsed_model):
        parsed_model.config.job_cluster_config = None
        parsed_model.config.cluster_id = "test"
        assert PythonWorkflowConfigCompiler.cluster_settings(parsed_model) == {
            "existing_cluster_id": "test"
        }

    def test_cluster_settings__job_cluster_config(self, parsed_model):
        parsed_model.config.job_cluster_config = {"foo": "bar"}
        assert PythonWorkflowConfigCompiler.cluster_settings(parsed_model) == {
            "new_cluster": {"foo": "bar"}
        }

    def test_compile__golden_path(self):
        workflow_settings = {"foo": "bar"}
        workflow_spec = {"baz": "qux"}
        post_hook_tasks = [{"task_key": "post_hook"}]
        compiler = PythonWorkflowConfigCompiler(
            workflow_settings, workflow_spec, "existing_job_id", post_hook_tasks
        )
        path = "path"
        assert compiler.compile(path) == (
            {
                "tasks": [
                    {
                        "task_key": "inner_notebook",
                        "notebook_task": {"notebook_path": path, "source": "WORKSPACE"},
                        "foo": "bar",
                    }
                ]
                + post_hook_tasks,
                "baz": "qux",
            },
            "existing_job_id",
        )

    def test_create__no_python_job_config(self, parsed_model):
        parsed_model.config.python_job_config = None
        parsed_model.config.job_cluster_config = None
        parsed_model.config.cluster_id = "test"
        compiler = PythonWorkflowConfigCompiler.create(parsed_model)
        assert compiler.task_settings == {"existing_cluster_id": "test"}
        assert compiler.workflow_spec == {}
        assert compiler.existing_job_id is None
        assert compiler.post_hook_tasks == []

    def test_create__python_job_config(self, parsed_model):
        parsed_model.config.python_job_config.dict.return_value = {"bar": "baz"}
        parsed_model.config.python_job_config.additional_task_settings = {"foo": "bar"}
        parsed_model.config.python_job_config.existing_job_id = 1
        parsed_model.config.python_job_config.name = "name"
        parsed_model.config.python_job_config.post_hook_tasks = [{"task_key": "post_hook"}]
        parsed_model.config.job_cluster_config = None
        parsed_model.config.cluster_id = None
        compiler = PythonWorkflowConfigCompiler.create(parsed_model)
        assert compiler.task_settings == {"foo": "bar"}
        assert compiler.workflow_spec == {"name": "name", "bar": "baz"}
        assert compiler.existing_job_id == 1
        assert compiler.post_hook_tasks == [{"task_key": "post_hook"}]


class TestPythonWorkflowCreator:
    @pytest.fixture
    def workflows(self):
        return Mock()

    @pytest.fixture
    def workflow_spec(self):
        return {"name": "bar"}

    @pytest.fixture
    def existing_job_id(self):
        return "test"

    @pytest.fixture
    def creator(self, workflows):
        return PythonWorkflowCreator(workflows)

    def test_create_or_update__existing_job_id(
        self, creator, workflows, workflow_spec, existing_job_id
    ):
        job_id = creator.create_or_update(workflow_spec, existing_job_id)
        assert job_id == existing_job_id
        workflows.update_job_settings.assert_called_once_with(existing_job_id, workflow_spec)

    def test_create_or_update__no_existing_job_creates_one(self, creator, workflows, workflow_spec):
        workflows.search_by_name.return_value = []
        workflows.create.return_value = "job_id"

        job_id = creator.create_or_update(workflow_spec, "")
        assert job_id == "job_id"
        workflows.create.assert_called_once_with(workflow_spec)

    def test_create_or_update__existing_job(self, creator, workflows, workflow_spec):
        workflows.search_by_name.return_value = [{"job_id": "job_id"}]

        job_id = creator.create_or_update(workflow_spec, "")
        assert job_id == "job_id"
        workflows.create.assert_not_called()
        workflows.search_by_name.assert_called_once_with("bar")
        workflows.update_job_settings.assert_called_once_with("job_id", workflow_spec)
