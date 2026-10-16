from unittest.mock import Mock, patch

import freezegun
import pytest
from databricks.sdk.errors import InvalidState
from databricks.sdk.service.compute import State
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import ClusterApi


class TestClusterApi:
    @pytest.fixture
    def library_api(self):
        return Mock()

    @pytest.fixture
    def workspace_client(self):
        mock = Mock()
        return mock

    @pytest.fixture
    def api(self, workspace_client, library_api):
        return ClusterApi(workspace_client, library_api)

    def test_status__exception(self, api, workspace_client):
        workspace_client.clusters.get.side_effect = Exception("API Error")
        with pytest.raises(DbtRuntimeError, match="Error getting status of cluster"):
            api.status("cluster_id")

    def test_status__success(self, api, workspace_client):
        mock_cluster = Mock()
        mock_cluster.state = State.RUNNING
        workspace_client.clusters.get.return_value = mock_cluster

        state = api.status("cluster_id")
        assert state == "RUNNING"
        workspace_client.clusters.get.assert_called_once_with(cluster_id="cluster_id")

    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__success(self, mock_sleep, api, workspace_client, library_api):
        # Mock cluster states: first PENDING, then RUNNING
        mock_cluster_pending = Mock()
        mock_cluster_pending.state = State.PENDING
        mock_cluster_running = Mock()
        mock_cluster_running.state = State.RUNNING

        workspace_client.clusters.get.side_effect = [mock_cluster_pending, mock_cluster_running]
        library_api.get_cluster_libraries_status.return_value = {"library_statuses": []}
        library_api.all_libraries_installed.return_value = True

        api.wait_for_cluster("cluster_id")
        assert mock_sleep.call_count == 1

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=900)
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_wait_for_cluster__timeout(self, mock_sleep, api, workspace_client):
        mock_cluster = Mock()
        mock_cluster.state = State.PENDING
        workspace_client.clusters.get.return_value = mock_cluster

        with pytest.raises(DbtRuntimeError, match="restart timed out"):
            api.wait_for_cluster("cluster_id")

    def test_start__success(self, api, workspace_client, library_api):
        # Mock cluster starting from TERMINATED state
        mock_cluster_terminated = Mock()
        mock_cluster_terminated.state = State.TERMINATED
        mock_cluster_running = Mock()
        mock_cluster_running.state = State.RUNNING

        workspace_client.clusters.get.side_effect = [mock_cluster_terminated, mock_cluster_running]
        library_api.get_cluster_libraries_status.return_value = {"library_statuses": []}
        library_api.all_libraries_installed.return_value = True

        api.start("cluster_id")
        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    @pytest.fixture
    def clock(self):
        elapsed = [0.0]

        def sleep(seconds):
            elapsed[0] += seconds

        with (
            patch("dbt.adapters.databricks.api_client.time.time", side_effect=lambda: elapsed[0]),
            patch("dbt.adapters.databricks.api_client.time.sleep", side_effect=sleep),
        ):
            yield elapsed

    @staticmethod
    def _cluster_states(workspace_client, states):
        remaining = list(states)
        last = [None]

        def get(**kwargs):
            if remaining:
                last[0] = remaining.pop(0)
            return Mock(state=State(last[0]))

        def start(**kwargs):
            if last[0] != "TERMINATED":
                raise InvalidState(f"Cluster is in unexpected state {last[0]}")

        workspace_client.clusters.get.side_effect = get
        workspace_client.clusters.start.side_effect = start

    def test_start__waits_for_terminating_cluster_before_starting(
        self, api, workspace_client, library_api, clock
    ):
        self._cluster_states(
            workspace_client, ["TERMINATING", "TERMINATING", "TERMINATED", "PENDING", "RUNNING"]
        )
        library_api.all_libraries_installed.return_value = True

        api.start("cluster_id")

        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    def test_start__terminating_cluster_started_elsewhere(
        self, api, workspace_client, library_api, clock
    ):
        self._cluster_states(workspace_client, ["TERMINATING", "PENDING", "RUNNING"])
        library_api.all_libraries_installed.return_value = True

        api.start("cluster_id")

        workspace_client.clusters.start.assert_not_called()

    @pytest.mark.parametrize("failed_state", ["ERROR", "UNKNOWN"])
    def test_start__terminating_cluster_enters_failed_state(
        self, api, workspace_client, clock, failed_state
    ):
        self._cluster_states(workspace_client, ["TERMINATING", failed_state])

        with pytest.raises(DbtRuntimeError, match=f"is in {failed_state} state"):
            api.start("cluster_id")
        workspace_client.clusters.start.assert_not_called()

    @pytest.mark.parametrize("failed_state", ["ERROR", "UNKNOWN"])
    def test_wait_for_cluster__fails_fast_on_failed_state(
        self, api, workspace_client, clock, failed_state
    ):
        self._cluster_states(workspace_client, ["PENDING", failed_state])

        with pytest.raises(DbtRuntimeError, match=f"is in {failed_state} state"):
            api.wait_for_cluster("cluster_id")
        assert clock[0] == 5

    def test_start__terminating_wait_shares_start_timeout(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATING"] * 170 + ["TERMINATED", "RESTARTING"])

        with pytest.raises(DbtRuntimeError, match="restart timed out"):
            api.start("cluster_id")
        assert clock[0] <= api.max_cluster_start_time + 5
