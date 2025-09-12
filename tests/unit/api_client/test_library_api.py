from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import LibraryFullStatus, LibraryInstallStatus

from dbt.adapters.databricks.api_client import LibraryApi


class TestLibraryApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return LibraryApi(workspace_client)

    def test_get_cluster_libraries_status__exception(self, api, workspace_client):
        workspace_client.libraries.cluster_status.side_effect = Exception("API Error")

        with pytest.raises(Exception) as exc_info:
            api.get_cluster_libraries_status("cluster_id")

        assert "Error getting status of libraries of a cluster" in str(exc_info.value)

    def test_get_cluster_libraries_status__success(self, api, workspace_client):
        cluster_id = "cluster_id"

        # Mock SDK response
        mock_lib_status1 = Mock(spec=LibraryFullStatus)
        mock_lib_status1.status = LibraryInstallStatus.INSTALLED
        mock_lib_status2 = Mock(spec=LibraryFullStatus)
        mock_lib_status2.status = LibraryInstallStatus.PENDING

        workspace_client.libraries.cluster_status.return_value = [
            mock_lib_status1,
            mock_lib_status2,
        ]

        result = api.get_cluster_libraries_status(cluster_id)
        expected_response = {"library_statuses": [{"status": "INSTALLED"}, {"status": "PENDING"}]}

        assert result == expected_response
        workspace_client.libraries.cluster_status.assert_called_once_with(cluster_id=cluster_id)

    def test_all_libraries_installed__true(self, api, workspace_client):
        # Mock SDK response with all INSTALLED libraries
        mock_lib_status = Mock(spec=LibraryFullStatus)
        mock_lib_status.status = LibraryInstallStatus.INSTALLED
        workspace_client.libraries.cluster_status.return_value = [mock_lib_status]

        result = api.all_libraries_installed("cluster_id")
        assert result is True

    def test_all_libraries_installed__false(self, api, workspace_client):
        # Mock SDK response with PENDING library
        mock_lib_status = Mock(spec=LibraryFullStatus)
        mock_lib_status.status = LibraryInstallStatus.PENDING
        workspace_client.libraries.cluster_status.return_value = [mock_lib_status]

        result = api.all_libraries_installed("cluster_id")
        assert result is False

    def test_library_statuses_not_present(self, api, workspace_client):
        # Mock SDK response with no libraries
        workspace_client.libraries.cluster_status.return_value = []

        result = api.all_libraries_installed("abc-123")
        assert result is True
