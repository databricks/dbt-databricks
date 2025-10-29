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
    def library_api(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client, cluster_api, library_api):
        return CommandContextApi(workspace_client, cluster_api, library_api)

    def test_create__exception(self, api, cluster_api, library_api, workspace_client):
        cluster_api.status.return_value = "RUNNING"
        library_api.all_libraries_installed.return_value = True
        workspace_client.command_execution.create.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.create("cluster_id")

        assert "Error creating an execution context" in str(exc_info.value)

    def test_create__cluster_running(self, api, cluster_api, library_api, workspace_client):
        cluster_api.status.return_value = "RUNNING"
        library_api.all_libraries_installed.return_value = True

        # Mock the Wait object returned by create()
        # The Wait object's result() method returns a response with id attribute
        mock_waiter = Mock()
        mock_response = Mock()
        mock_response.id = "context_id"
        mock_waiter.result.return_value = mock_response
        workspace_client.command_execution.create.return_value = mock_waiter

        context_id = api.create("cluster_id")

        assert context_id == "context_id"
        workspace_client.command_execution.create.assert_called_once()
        cluster_api.wait_for_cluster.assert_not_called()

    def test_create__cluster_running_with_pending_libraries(
        self, api, cluster_api, library_api, workspace_client
    ):
        cluster_api.status.return_value = "RUNNING"
        library_api.all_libraries_installed.return_value = False

        # Mock the Wait object returned by create()
        # The Wait object's result() method returns a response with id attribute
        mock_waiter = Mock()
        mock_response = Mock()
        mock_response.id = "context_id"
        mock_waiter.result.return_value = mock_response
        workspace_client.command_execution.create.return_value = mock_waiter

        context_id = api.create("cluster_id")

        assert context_id == "context_id"
        workspace_client.command_execution.create.assert_called_once()
        cluster_api.wait_for_cluster.assert_called_once_with("cluster_id")

    def test_create__cluster_terminated(self, api, cluster_api, workspace_client):
        cluster_api.status.return_value = "TERMINATED"

        # Mock the Wait object returned by create()
        # The Wait object's result() method returns a response with id attribute
        mock_waiter = Mock()
        mock_response = Mock()
        mock_response.id = "context_id"
        mock_waiter.result.return_value = mock_response
        workspace_client.command_execution.create.return_value = mock_waiter

        api.create("cluster_id")

        cluster_api.start.assert_called_once_with("cluster_id")

    def test_create__cluster_pending(self, api, cluster_api, workspace_client):
        cluster_api.status.return_value = "PENDING"

        # Mock the Wait object returned by create()
        # The Wait object's result() method returns a response with id attribute
        mock_waiter = Mock()
        mock_response = Mock()
        mock_response.id = "context_id"
        mock_waiter.result.return_value = mock_response
        workspace_client.command_execution.create.return_value = mock_waiter

        api.create("cluster_id")

        cluster_api.wait_for_cluster.assert_called_once_with("cluster_id")

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
