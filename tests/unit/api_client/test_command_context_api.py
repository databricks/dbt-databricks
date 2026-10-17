from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import CommandContextApi


class TestCommandContextApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def cluster_api(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client, cluster_api):
        return CommandContextApi(workspace_client, cluster_api)

    def test_create__exception(self, api, workspace_client):
        workspace_client.command_execution.create.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.create("cluster_id")

        assert "Error creating an execution context" in str(exc_info.value)

    def test_create__ensures_cluster_running(self, api, cluster_api, workspace_client):
        # The Wait object has context_id immediately available and result() waits for RUNNING
        mock_waiter = Mock()
        mock_waiter.context_id = "context_id"
        mock_response = Mock()
        mock_response.id = "context_id"
        mock_waiter.result.return_value = mock_response
        workspace_client.command_execution.create.return_value = mock_waiter

        context_id = api.create("cluster_id")

        assert context_id == "context_id"
        cluster_api.ensure_running.assert_called_once_with("cluster_id")
        workspace_client.command_execution.create.assert_called_once()

    def test_create__cluster_not_ready(self, api, cluster_api, workspace_client):
        cluster_api.ensure_running.side_effect = DbtRuntimeError("Cluster is in ERROR state")

        with pytest.raises(DbtRuntimeError, match="is in ERROR state"):
            api.create("cluster_id")

        workspace_client.command_execution.create.assert_not_called()

    def test_destroy__exception(self, api, workspace_client):
        workspace_client.command_execution.destroy.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.destroy("cluster_id", "context_id")

        assert "Error deleting an execution context" in str(exc_info.value)

    def test_destroy__success(self, api, workspace_client):
        api.destroy("cluster_id", "context_id")

        workspace_client.command_execution.destroy.assert_called_once_with(
            cluster_id="cluster_id", context_id="context_id"
        )
