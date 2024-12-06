from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import CommandStatus, ResultType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import CommandApi, CommandExecution
from tests.unit.api_client.api_test_base import ApiTestBase


class TestCommandApi(ApiTestBase):
    @pytest.fixture
    def api(self, client):
        return CommandApi(client, 0)

    @pytest.fixture
    def command_client(self, client):
        return client.command_execution

    @pytest.fixture
    def execution(self):
        return CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

    def test_execute__error(self, api, command_client):
        command_client.execute_and_wait.return_value = Mock(
            status=CommandStatus.ERROR, results={"data": "fail"}
        )
        with pytest.raises(DbtRuntimeError, match="Error creating a command"):
            api.execute("cluster_id", "context_id", "command")

    def test_execute__success(self, api, command_client, execution):
        command_client.execute_and_wait.return_value = Mock(
            status=CommandStatus.FINISHED, id="command_id"
        )
        assert api.execute("cluster_id", "context_id", "command") == execution

    def test_cancel__error(self, api, command_client):
        command_client.wait_command_status_command_execution_cancelled.return_value = Mock(
            status=CommandStatus.ERROR, results={"data": "fail"}
        )
        with pytest.raises(DbtRuntimeError, match="Command failed to cancel"):
            api.cancel(Mock())

    def test_cancel__success(self, api, command_client, execution):
        command_client.wait_command_status_command_execution_cancelled.return_value = Mock(
            status=CommandStatus.FINISHED
        )
        # Raises no error
        api.cancel(execution)

    def test_poll_for_completion__error(self, api, command_client):
        command_client.wait_command_status_command_execution_finished_or_error.return_value = Mock(
            status=CommandStatus.ERROR
        )
        with pytest.raises(DbtRuntimeError, match="Command failed with"):
            api.poll_for_completion(Mock())

    def test_poll_for_completion__alt_error(self, api, command_client):
        command_client.wait_command_status_command_execution_finished_or_error.return_value = Mock(
            status=CommandStatus.FINISHED,
            results=Mock(result_type=ResultType.ERROR, cause="failure"),
        )
        with pytest.raises(DbtRuntimeError, match="Python model failed with traceback"):
            api.poll_for_completion(Mock())

    def test_poll_for_completion__success(self, api, command_client):
        command_client.wait_command_status_command_execution_finished_or_error.return_value = Mock(
            status=CommandStatus.FINISHED
        )
        # Raises no error
        api.poll_for_completion(Mock())
