from unittest.mock import Mock, patch

import freezegun
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import ClusterApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestClusterApi(ApiTestBase):
    @pytest.fixture
    def library_api(self):
        return Mock()

    @pytest.fixture
    def api(self, session, host, library_api):
        return ClusterApi(session, host, library_api)

    def test_status__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.status("cluster_id"), session)

    def test_status__200(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "running"}
        state = api.status("cluster_id")
        assert state == "RUNNING"
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/clusters/get", json={"cluster_id": "cluster_id"}, params=None
        )

    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__success(self, _, api, session, library_api):
        session.get.return_value.status_code = 200
        session.get.return_value.json.side_effect = [{"state": "pending"}, {"state": "running"}]
        library_api.get_cluster_libraries_status.return_value = {"library_statuses": []}
        api.wait_for_cluster("cluster_id")

    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster_with_installed_library__success(
        self, mock_sleep, api, session, library_api
    ):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "running"}
        library_api.get_cluster_libraries_status.return_value = {
            "library_statuses": [{"status": "INSTALLED"}]
        }
        api.wait_for_cluster("cluster_id")
        mock_sleep.assert_not_called()

    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster_with_pending_library__success(
        self, mock_sleep, api, session, library_api
    ):
        session.get.return_value.status_code = 200
        session.get.return_value.json.side_effect = [{"state": "running"}, {"state": "running"}]
        library_api.all_libraries_installed.side_effect = [False, True]
        api.wait_for_cluster("cluster_id")
        mock_sleep.assert_called_with(5)

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=900)
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__timeout(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "pending"}
        with pytest.raises(DbtRuntimeError):
            api.wait_for_cluster("cluster_id")

    def test_start__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.start("cluster_id"), session)

    def test_start__200(self, api, session, host, library_api):
        session.post.return_value.status_code = 200
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "running"}
        library_api.get_cluster_libraries_status.return_value = {"library_statuses": []}
        api.start("cluster_id")
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/clusters/start", json={"cluster_id": "cluster_id"}, params=None
        )
