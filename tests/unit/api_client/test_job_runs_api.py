from unittest.mock import Mock, patch

import freezegun
import pytest
from databricks.sdk.service.jobs import (
    JobPermissionLevel,
    Run,
    RunLifeCycleState,
    RunResultState,
    RunState,
)
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import JobRunsApi


class TestJobRunsApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return JobRunsApi(workspace_client, 1, 2)

    def test_submit__exception(self, api, workspace_client):
        workspace_client.jobs.submit.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.submit("run_name", {})

        assert "Error creating python run" in str(exc_info.value)

    def test_submit__success(self, api, workspace_client):
        mock_submit_result = Mock()
        mock_submit_result.run_id = 12345
        workspace_client.jobs.submit.return_value = mock_submit_result

        result = api.submit("run_name", {"task_key": "test"})

        assert result == "12345"
        workspace_client.jobs.submit.assert_called_once()
        # Verify the call was made with proper parameters
        call_kwargs = workspace_client.jobs.submit.call_args[1]
        assert call_kwargs["run_name"] == "run_name"
        assert len(call_kwargs["tasks"]) == 1

    def test_submit__job_spec_conversion(self, api, workspace_client):
        mock_submit_result = Mock()
        mock_submit_result.run_id = 12345
        workspace_client.jobs.submit.return_value = mock_submit_result

        # Test job_spec with fields that need conversion
        job_spec = {
            "task_key": "test_task",
            "notebook_task": {"notebook_path": "/path/to/notebook"},
            "cluster_id": "test-cluster-id",  # Should be converted to existing_cluster_id
            "libraries": [{"pypi": {"package": "requests"}}],
            "access_control_list": [
                {"user_name": "user", "permission_level": "IS_OWNER"}
            ],  # Should be moved to submission level
            "queue": {"enabled": True},  # Should be moved to submission level
        }

        result = api.submit("test_run", job_spec)

        assert result == "12345"
        workspace_client.jobs.submit.assert_called_once()

        # Verify the call was made with proper parameters
        call_kwargs = workspace_client.jobs.submit.call_args[1]
        assert call_kwargs["run_name"] == "test_run"
        assert len(call_kwargs["tasks"]) == 1

        # Check that task conversion worked correctly
        task = call_kwargs["tasks"][0]
        assert task.task_key == "test_task"
        assert task.notebook_task.notebook_path == "/path/to/notebook"
        assert task.existing_cluster_id == "test-cluster-id"  # Converted from cluster_id
        assert len(task.libraries) == 1
        assert task.libraries[0].pypi.package == "requests"

        # Check that submission-level parameters were handled correctly
        assert "queue" in call_kwargs
        assert call_kwargs["queue"].enabled is True
        assert "access_control_list" in call_kwargs
        assert len(call_kwargs["access_control_list"]) == 1
        assert call_kwargs["access_control_list"][0].user_name == "user"
        assert call_kwargs["access_control_list"][0].permission_level == JobPermissionLevel.IS_OWNER

    def test_get_run_info__exception(self, api, workspace_client):
        workspace_client.jobs.get_run.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.get_run_info("123")

        assert "Error getting run info" in str(exc_info.value)

    def test_get_run_info__success(self, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_run.as_dict.return_value = {"job_id": 123, "run_id": 456}
        workspace_client.jobs.get_run.return_value = mock_run

        result = api.get_run_info("456")

        assert result == {"job_id": 123, "run_id": 456}
        workspace_client.jobs.get_run.assert_called_once_with(run_id=456)

    def test_get_job_id_from_run_id__job_id_exists(self, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_run.as_dict.return_value = {"job_id": 123, "run_id": 456}
        workspace_client.jobs.get_run.return_value = mock_run

        result = api.get_job_id_from_run_id("456")
        assert result == "123"

    def test_get_job_id_from_run_id__no_job_id(self, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_run.as_dict.return_value = {"run_id": 456}
        workspace_client.jobs.get_run.return_value = mock_run

        with pytest.raises(DbtRuntimeError, match="Could not get job_id from run_id 456"):
            api.get_job_id_from_run_id("456")

    def test_cancel__exception(self, api, workspace_client):
        workspace_client.jobs.cancel_run.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.cancel("123")

        assert "Cancel run 123 failed" in str(exc_info.value)

    def test_cancel__success(self, api, workspace_client):
        api.cancel("123")
        workspace_client.jobs.cancel_run.assert_called_once_with(run_id=123)

    def test_poll_for_completion__exception(self, api, workspace_client):
        workspace_client.jobs.get_run.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.poll_for_completion("123")

        assert "Error polling for completion" in str(exc_info.value)

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=3)
    @patch("time.sleep")
    def test_poll_for_completion__exceed_timeout(self, _, api, workspace_client):
        # Mock a run that never reaches terminal state
        mock_run = Mock(spec=Run)
        mock_state = Mock(spec=RunState)
        mock_state.life_cycle_state = RunLifeCycleState.RUNNING
        mock_run.state = mock_state
        workspace_client.jobs.get_run.return_value = mock_run

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("123")

        assert "Python model run timed out" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__error_handling_bailout(self, _, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_state = Mock(spec=RunState)
        mock_state.life_cycle_state = RunLifeCycleState.INTERNAL_ERROR
        mock_state.state_message = "error"
        mock_run.state = mock_state
        mock_run.run_id = 123
        workspace_client.jobs.get_run.return_value = mock_run

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("123")

        assert "Python model run ended in state INTERNAL_ERROR" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__cancelled(self, _, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_state = Mock(spec=RunState)
        mock_state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_state.result_state = RunResultState.CANCELED
        mock_state.state_message = "cancelled by user"
        mock_run.state = mock_state
        workspace_client.jobs.get_run.return_value = mock_run

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("123")

        assert "Python model run ended in result_state CANCELED" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__success(self, _, api, workspace_client):
        mock_run = Mock(spec=Run)
        mock_state = Mock(spec=RunState)
        mock_state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_state.result_state = RunResultState.SUCCESS
        mock_run.state = mock_state
        workspace_client.jobs.get_run.return_value = mock_run

        # Should complete without raising an exception
        api.poll_for_completion("123")

        workspace_client.jobs.get_run.assert_called_with(run_id=123)
