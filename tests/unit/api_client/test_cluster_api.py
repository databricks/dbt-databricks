import pytest
from dbt.adapters.databricks.api_client import ClusterApi
from dbt_common.exceptions import DbtRuntimeError
from mock import Mock


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
