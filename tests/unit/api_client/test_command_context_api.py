import pytest
from mock import Mock

from dbt.adapters.databricks.api_client import CommandContextApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestCommandContextApi(ApiTestBase):
    @pytest.fixture
    def cluster_api(self):
        return Mock()

    @pytest.fixture
    def api(self, session, host, cluster_api):
        return CommandContextApi(session, host, cluster_api)

    def test_create__non_200(self, api, cluster_api, session):
        cluster_api.status.return_value = "RUNNING"
        self.assert_non_200_raises_error(lambda: api.create("cluster_id"), session)

    def test_create__cluster_running(self, api, cluster_api, session):
        cluster_api.status.return_value = "RUNNING"
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"id": "context_id"}
        id = api.create("cluster_id")
        session.post.assert_called_once_with(
            "https://host/api/1.2/contexts/create",
            json={"clusterId": "cluster_id", "language": "python"},
            params=None,
        )
        assert id == "context_id"

    def test_create__cluster_terminated(self, api, cluster_api, session):
        cluster_api.status.return_value = "TERMINATED"
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"id": "context_id"}
        api.create("cluster_id")

        cluster_api.start.assert_called_once_with("cluster_id")

    def test_create__cluster_pending(self, api, cluster_api, session):
        cluster_api.status.return_value = "PENDING"
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"id": "context_id"}
        api.create("cluster_id")

        cluster_api.wait_for_cluster.assert_called_once_with("cluster_id")

    def test_destroy__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.destroy("cluster_id", "context_id"), session)

    def test_destroy__200(self, api, session):
        session.post.return_value.status_code = 200
        api.destroy("cluster_id", "context_id")

        session.post.assert_called_once_with(
            "https://host/api/1.2/contexts/destroy",
            json={"clusterId": "cluster_id", "contextId": "context_id"},
            params=None,
        )
