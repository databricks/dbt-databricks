import freezegun
import pytest
from dbt.adapters.databricks.api_client import ClusterApi
from dbt_common.exceptions import DbtRuntimeError
from mock import Mock
from mock import patch


class TestClusterApi:
    @pytest.fixture
    def session(self):
        return Mock()

    @pytest.fixture
    def host(self):
        return "host"

    @pytest.fixture
    def api(self, session, host):
        return ClusterApi(session, host)

    def test_status__non_200(self, api, session):
        session.get.return_value.status_code = 500
        with pytest.raises(DbtRuntimeError):
            api.status("cluster_id")

    def test_status__200(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "running"}
        state = api.status("cluster_id")
        assert state == "RUNNING"
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/clusters/get", json={"cluster_id": "cluster_id"}, params=None
        )

    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__success(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.side_effect = [{"state": "pending"}, {"state": "running"}]
        api.wait_for_cluster("cluster_id")

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=900)
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__timeout(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "pending"}
        with pytest.raises(DbtRuntimeError):
            api.wait_for_cluster("cluster_id")

    def test_start__non_200(self, api, session):
        session.get.return_value.status_code = 500
        with pytest.raises(DbtRuntimeError):
            api.start("cluster_id")

    def test_start__200(self, api, session, host):
        session.post.return_value.status_code = 200
        api.start("cluster_id")
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/clusters/start", json={"cluster_id": "cluster_id"}, params=None
        )
