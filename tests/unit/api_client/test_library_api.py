import pytest

from dbt.adapters.databricks.api_client import LibraryApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestLibraryApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return LibraryApi(session, host)

    def test_get_cluster_libraries_status__non_200(self, api, session):
        self.assert_non_200_raises_error(
            lambda: api.get_cluster_libraries_status("cluster_id"), session
        )

    def test_get_cluster_libraries_status__200(self, api, session, host):
        cluster_id = "cluster_id"
        expected_response = {"library_statuses": [{"status": "INSTALLED"}, {"status": "PENDING"}]}
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = expected_response

        result = api.get_cluster_libraries_status(cluster_id)
        assert result == expected_response
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/libraries/cluster-status",
            json={"cluster_id": cluster_id},
            params=None,
        )

    def test_all_libraries_installed__true(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"library_statuses": [{"status": "INSTALLED"}]}

        result = api.all_libraries_installed("cluster_id")
        assert result is True

    def test_all_libraries_installed__false(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"library_statuses": [{"status": "PENDING"}]}

        result = api.all_libraries_installed("cluster_id")
        assert result is False
