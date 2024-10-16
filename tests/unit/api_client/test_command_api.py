import freezegun
import pytest
from dbt.adapters.databricks.api_client import CommandApi
from dbt.adapters.databricks.api_client import CommandExecution
from dbt_common.exceptions import DbtRuntimeError
from mock import Mock
from mock import patch
from tests.unit.api_client.api_test_base import ApiTestBase


class TestCommandApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return CommandApi(session, host, 1, 2)

    @pytest.fixture
    def execution(self):
        return CommandExecution(
            command_id="command_id", cluster_id="cluster_id", context_id="context_id"
        )

    def test_execute__non_200(self, api, session):
        self.assert_non_200_raises_error(
            lambda: api.execute("cluster_id", "context_id", "command"), session
        )

    def test_execute__200(self, api, session, host, execution):
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"id": "command_id"}
        assert api.execute("cluster_id", "context_id", "command") == execution
        session.post.assert_called_once_with(
            f"https://{host}/api/1.2/commands/execute",
            json={
                "clusterId": "cluster_id",
                "contextId": "context_id",
                "command": "command",
                "language": "python",
            },
            params=None,
        )

    def test_cancel__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.cancel(Mock()), session)

    def test_cancel__200(self, api, session, host, execution):
        session.post.return_value.status_code = 200
        api.cancel(execution)
        session.post.assert_called_once_with(
            f"https://{host}/api/1.2/commands/cancel",
            json={
                "commandId": "command_id",
                "clusterId": "cluster_id",
                "contextId": "context_id",
            },
            params=None,
        )

    def test_poll_for_completion__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.poll_for_completion(Mock()), session)

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=3)
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__exceed_timeout(self, _, api):
        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion(Mock())

        assert "Python model run timed out" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__error_handling(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "status": "Error",
            "results": {"data": "fail"},
        }

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion(Mock())

        assert "Python model run ended in state Error" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__200(self, _, api, session, host, execution):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "status": "Finished",
        }

        api.poll_for_completion(execution)

        session.get.assert_called_once_with(
            f"https://{host}/api/1.2/commands/status",
            params={
                "clusterId": execution.cluster_id,
                "contextId": execution.context_id,
                "commandId": execution.command_id,
            },
            json=None,
        )
