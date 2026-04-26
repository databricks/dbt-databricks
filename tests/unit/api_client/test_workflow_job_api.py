from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.jobs import BaseJob, CreateResponse, JobSettings, QueueSettings, Task
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import WorkflowJobApi


class TestWorkflowJobApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return WorkflowJobApi(workspace_client)

    def test_search_by_name__exception(self, api, workspace_client):
        workspace_client.jobs.list.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.search_by_name("test_job")

        assert "Error fetching job by name" in str(exc_info.value)

    def test_search_by_name__success(self, api, workspace_client):
        mock_job = Mock(spec=BaseJob)
        mock_job.as_dict.return_value = {"job_id": 123, "name": "test_job"}
        workspace_client.jobs.list.return_value = [mock_job]

        result = api.search_by_name("test_job")

        assert result == [{"job_id": 123, "name": "test_job"}]
        workspace_client.jobs.list.assert_called_once_with(name="test_job")

    def test_search_by_name__empty_results(self, api, workspace_client):
        workspace_client.jobs.list.return_value = []

        result = api.search_by_name("nonexistent_job")

        assert result == []
        workspace_client.jobs.list.assert_called_once_with(name="nonexistent_job")

    def test_create__exception(self, api, workspace_client):
        workspace_client.jobs.create.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.create({"name": "test_job"})

        assert "Error creating Workflow" in str(exc_info.value)

    def test_create__success(self, api, workspace_client):
        mock_create_response = Mock(spec=CreateResponse)
        mock_create_response.job_id = 456
        workspace_client.jobs.create.return_value = mock_create_response

        job_spec = {"name": "test_job", "tasks": []}
        result = api.create(job_spec)

        assert result == "456"
        workspace_client.jobs.create.assert_called_once()
        call_kwargs = workspace_client.jobs.create.call_args[1]
        assert call_kwargs["name"] == "test_job"
        assert call_kwargs.get("tasks") in (None, [])

    def test_create__job_spec_conversion(self, api, workspace_client):
        mock_create_response = Mock(spec=CreateResponse)
        mock_create_response.job_id = 789
        workspace_client.jobs.create.return_value = mock_create_response

        # Test job_spec with tasks that need cluster_id conversion
        job_spec = {
            "name": "test_job",
            "tasks": [
                {
                    "task_key": "task1",
                    "notebook_task": {"notebook_path": "/path/to/notebook"},
                    "cluster_id": "test-cluster-id",  # Should be converted to existing_cluster_id
                    "libraries": [{"pypi": {"package": "requests"}}],
                },
                {
                    "task_key": "task2",
                    "notebook_task": {"notebook_path": "/path/to/notebook2"},
                    "existing_cluster_id": "already-correct",  # Should remain unchanged
                },
            ],
        }

        result = api.create(job_spec)

        assert result == "789"
        workspace_client.jobs.create.assert_called_once()

        # Tasks are converted to SDK Task dataclasses via JobSettings.from_dict().as_shallow_dict().
        # The SDK's jobs.create() calls v.as_dict() on each task internally, so
        # we verify: (a) they are Task instances, (b) attributes are correct,
        # (c) .as_dict() works (proving the original AttributeError is fixed).
        call_kwargs = workspace_client.jobs.create.call_args[1]
        assert call_kwargs["name"] == "test_job"
        tasks = call_kwargs["tasks"]
        assert len(tasks) == 2

        task1 = tasks[0]
        assert isinstance(task1, Task)
        assert task1.task_key == "task1"
        assert task1.existing_cluster_id == "test-cluster-id"
        # Verify .as_dict() works — this was the root cause of the bug
        task1_dict = task1.as_dict()
        assert task1_dict["task_key"] == "task1"
        assert task1_dict["existing_cluster_id"] == "test-cluster-id"

        task2 = tasks[1]
        assert isinstance(task2, Task)
        assert task2.task_key == "task2"
        assert task2.existing_cluster_id == "already-correct"
        assert task2.as_dict()["existing_cluster_id"] == "already-correct"

    def test_create__invalid_job_spec_raises(self, api, workspace_client):
        """If JobSettings.from_dict() raises (e.g. on malformed input), create()
        wraps it in a DbtRuntimeError via the existing except block."""
        with patch(
            "dbt.adapters.databricks.api_client.JobSettings.from_dict",
            side_effect=Exception("malformed spec"),
        ):
            with pytest.raises(DbtRuntimeError) as exc_info:
                api.create({"name": "bad_job", "tasks": ["not-a-dict"]})

        assert "Error creating Workflow" in str(exc_info.value)
        workspace_client.jobs.create.assert_not_called()

    def test_update_job_settings__exception(self, api, workspace_client):
        workspace_client.jobs.reset.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.update_job_settings("123", {"name": "updated_job"})

        assert "Error updating Workflow" in str(exc_info.value)

    def test_update_job_settings__success(self, api, workspace_client):
        job_spec = {"name": "updated_job", "tasks": []}

        api.update_job_settings("123", job_spec)

        workspace_client.jobs.reset.assert_called_once()
        call_args = workspace_client.jobs.reset.call_args
        assert call_args[1]["job_id"] == 123
        # The new_settings should be a JobSettings object
        assert isinstance(call_args[1]["new_settings"], JobSettings)

    def test_run__exception(self, api, workspace_client):
        workspace_client.jobs.run_now.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.run("123")

        assert "Error triggering run for workflow" in str(exc_info.value)

    def test_run__success(self, api, workspace_client):
        mock_run_result = Mock()
        mock_run_result.run_id = 789
        workspace_client.jobs.run_now.return_value = mock_run_result

        result = api.run("123", enable_queueing=True)

        assert result == "789"
        workspace_client.jobs.run_now.assert_called_once()
        call_args = workspace_client.jobs.run_now.call_args
        assert call_args[1]["job_id"] == 123
        assert isinstance(call_args[1]["queue"], QueueSettings)
        assert call_args[1]["queue"].enabled is True

    def test_run__disable_queueing(self, api, workspace_client):
        mock_run_result = Mock()
        mock_run_result.run_id = 789
        workspace_client.jobs.run_now.return_value = mock_run_result

        result = api.run("123", enable_queueing=False)

        assert result == "789"
        call_args = workspace_client.jobs.run_now.call_args
        assert call_args[1]["queue"].enabled is False
