from unittest.mock import Mock, patch

import freezegun
import pytest
from databricks.sdk.service.compute import CommandStatus, CommandStatusResponse
from databricks.sdk.service.compute import Language as ComputeLanguage
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import CommandApi, CommandExecution


class TestCommandApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return CommandApi(workspace_client, 1, 2)

    @pytest.fixture
    def execution(self):
        return CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

    def test_execute__exception(self, api, workspace_client):
        workspace_client.command_execution.execute.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.execute("cluster_id", "context_id", "command")

        assert "Error creating a command" in str(exc_info.value)

    def test_execute__success(self, api, workspace_client, execution):
        # Mock the Wait object returned by execute()
        # The command_id is available immediately via __getattr__, not via result()
        mock_waiter = Mock()
        mock_waiter.command_id = "command_id"
        workspace_client.command_execution.execute.return_value = mock_waiter

        result = api.execute("cluster_id", "context_id", "command")

        assert result == execution
        workspace_client.command_execution.execute.assert_called_once_with(
            cluster_id="cluster_id",
            context_id="context_id",
            command="command",
            language=ComputeLanguage.PYTHON,
        )
        # result() should NOT be called - we access command_id directly
        mock_waiter.result.assert_not_called()

    def test_cancel__exception(self, api, workspace_client):
        workspace_client.command_execution.cancel.side_effect = Exception("API Error")
        execution = CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.cancel(execution)

        assert "Cancel command" in str(exc_info.value)

    def test_cancel__success(self, api, workspace_client, execution):
        api.cancel(execution)
        workspace_client.command_execution.cancel.assert_called_once_with(
            cluster_id="cluster_id", context_id="context_id", command_id="command_id"
        )

    def test_poll_for_completion__exception(self, api, workspace_client):
        workspace_client.command_execution.command_status.side_effect = Exception("API Error")
        execution = CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.poll_for_completion(execution)

        assert "Error polling for command completion" in str(exc_info.value)

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=3)
    @patch("time.sleep")
    def test_poll_for_completion__exceed_timeout(self, _, api, workspace_client):
        # Mock a command that never reaches terminal state
        mock_status_response = Mock(spec=CommandStatusResponse)
        mock_status_response.status = CommandStatus.RUNNING
        workspace_client.command_execution.command_status.return_value = mock_status_response

        execution = CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion(execution)

        assert "Command execution timed out" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__error_handling(self, _, api, workspace_client):
        mock_status_response = Mock(spec=CommandStatusResponse)
        mock_status_response.status = CommandStatus.ERROR
        mock_status_response.results = Mock()
        mock_status_response.results.data = "fail"
        workspace_client.command_execution.command_status.return_value = mock_status_response

        execution = CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion(execution)

        assert "Python model run ended in state Error" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__success(self, _, api, workspace_client, execution):
        mock_status_response = Mock(spec=CommandStatusResponse)
        mock_status_response.status = CommandStatus.FINISHED
        mock_status_response.results = Mock()
        mock_status_response.results.result_type = "finished"
        workspace_client.command_execution.command_status.return_value = mock_status_response

        # Should complete without raising an exception
        api.poll_for_completion(execution)

        workspace_client.command_execution.command_status.assert_called_with(
            cluster_id=execution.cluster_id,
            context_id=execution.context_id,
            command_id=execution.command_id,
        )

    @freezegun.freeze_time("2020-01-01")
    @patch("time.sleep")
    def test_poll_for_completion__finished_with_error(self, _, api, workspace_client, execution):
        mock_status_response = Mock(spec=CommandStatusResponse)
        mock_status_response.status = CommandStatus.FINISHED
        mock_status_response.results = Mock()
        mock_status_response.results.result_type = "error"
        mock_status_response.results.cause = "race condition"
        workspace_client.command_execution.command_status.return_value = mock_status_response

        with pytest.raises(DbtRuntimeError, match="Python model failed"):
            api.poll_for_completion(execution)
