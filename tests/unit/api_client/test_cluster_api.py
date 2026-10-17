from unittest.mock import Mock, patch

import pytest
from databricks.sdk.errors import InvalidState
from databricks.sdk.service.compute import State
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import ClusterApi


class TestClusterApi:
    @pytest.fixture
    def library_api(self):
        mock = Mock()
        mock.all_libraries_installed.return_value = True
        return mock

    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client, library_api):
        return ClusterApi(workspace_client, library_api)

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
        """Serve `states` from clusters.get, repeating the last; reject start unless TERMINATED.

        Each entry is a state name or a `(state name, state_message)` pair.
        """
        remaining = [s if isinstance(s, tuple) else (s, None) for s in states]
        last = [(None, None)]

        def get(**kwargs):
            if remaining:
                last[0] = remaining.pop(0)
            state, message = last[0]
            return Mock(state=State(state), state_message=message)

        def start(**kwargs):
            if last[0][0] != "TERMINATED":
                raise InvalidState(f"Cluster is in unexpected state {last[0][0]}")

        workspace_client.clusters.get.side_effect = get
        workspace_client.clusters.start.side_effect = start

    def test_ensure_running__get_error(self, api, workspace_client):
        workspace_client.clusters.get.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError, match="Error getting status of cluster"):
            api.ensure_running("cluster_id")

    def test_ensure_running__already_running(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["RUNNING"])

        api.ensure_running("cluster_id")

        workspace_client.clusters.get.assert_called_once_with(cluster_id="cluster_id")
        workspace_client.clusters.start.assert_not_called()
        assert clock[0] == 0

    def test_ensure_running__waits_for_libraries(self, api, workspace_client, library_api, clock):
        self._cluster_states(workspace_client, ["RUNNING"])
        library_api.all_libraries_installed.side_effect = [False, False, True]

        api.ensure_running("cluster_id")

        assert clock[0] == 10

    def test_ensure_running__pending(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["PENDING", "PENDING", "RUNNING"])

        api.ensure_running("cluster_id")

        workspace_client.clusters.start.assert_not_called()

    def test_ensure_running__timeout(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["PENDING"])

        with pytest.raises(DbtRuntimeError, match="restart timed out after 900 seconds"):
            api.ensure_running("cluster_id")

    def test_ensure_running__starts_terminated_cluster(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATED", "PENDING", "RUNNING"])

        api.ensure_running("cluster_id")

        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    def test_ensure_running__waits_for_terminating_cluster_before_starting(
        self, api, workspace_client, clock
    ):
        self._cluster_states(
            workspace_client, ["TERMINATING", "TERMINATING", "TERMINATED", "PENDING", "RUNNING"]
        )

        api.ensure_running("cluster_id")

        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    def test_ensure_running__terminating_cluster_started_elsewhere(
        self, api, workspace_client, clock
    ):
        self._cluster_states(workspace_client, ["TERMINATING", "PENDING", "RUNNING"])

        api.ensure_running("cluster_id")

        workspace_client.clusters.start.assert_not_called()

    def test_ensure_running__terminating_outlasts_timeout(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATING"])

        with pytest.raises(DbtRuntimeError, match="restart timed out"):
            api.ensure_running("cluster_id")
        workspace_client.clusters.start.assert_not_called()

    def test_ensure_running__terminating_shares_start_timeout(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATING"] * 179 + ["TERMINATED", "PENDING"])

        with pytest.raises(DbtRuntimeError, match="restart timed out"):
            api.ensure_running("cluster_id")
        assert clock[0] == api.max_cluster_start_time

    @pytest.mark.parametrize("failed_state", ["ERROR", "UNKNOWN"])
    @pytest.mark.parametrize("preceding_state", ["PENDING", "TERMINATING"])
    def test_ensure_running__fails_fast_on_failed_state(
        self, api, workspace_client, clock, failed_state, preceding_state
    ):
        self._cluster_states(
            workspace_client, [preceding_state, (failed_state, "Driver is unresponsive")]
        )

        with pytest.raises(
            DbtRuntimeError, match=f"is in {failed_state} state: Driver is unresponsive"
        ):
            api.ensure_running("cluster_id")
        workspace_client.clusters.start.assert_not_called()
        assert clock[0] == 5

    def test_ensure_running__failed_state_without_message(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["ERROR"])

        with pytest.raises(DbtRuntimeError, match=r"Cluster cluster_id is in ERROR state$"):
            api.ensure_running("cluster_id")

    def test_ensure_running__failed_state_after_start_is_not_treated_as_race(
        self, api, workspace_client, clock
    ):
        self._cluster_states(workspace_client, ["TERMINATED", "PENDING", "ERROR", "PENDING"])

        with pytest.raises(DbtRuntimeError, match="is in ERROR state"):
            api.ensure_running("cluster_id")

    def test_ensure_running__fails_fast_when_started_cluster_terminates(
        self, api, workspace_client, clock
    ):
        self._cluster_states(
            workspace_client,
            ["TERMINATED", "PENDING", ("TERMINATED", "Init script failure"), "PENDING"],
        )

        with pytest.raises(DbtRuntimeError, match="terminated while starting: Init script failure"):
            api.ensure_running("cluster_id")
        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    def test_ensure_running__waits_while_start_is_not_yet_reflected(
        self, api, workspace_client, clock
    ):
        self._cluster_states(workspace_client, ["TERMINATED", "TERMINATED", "PENDING", "RUNNING"])

        api.ensure_running("cluster_id")

        workspace_client.clusters.start.assert_called_once_with(cluster_id="cluster_id")

    def test_ensure_running__lost_start_race_keeps_waiting(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATED", "PENDING", "PENDING", "RUNNING"])
        workspace_client.clusters.start.side_effect = InvalidState("already starting")

        api.ensure_running("cluster_id")

        assert workspace_client.clusters.get.call_count == 4

    def test_ensure_running__start_error(self, api, workspace_client, clock):
        self._cluster_states(workspace_client, ["TERMINATED"])
        workspace_client.clusters.start.side_effect = Exception("Permission denied")

        with pytest.raises(DbtRuntimeError, match="Error starting cluster: Permission denied"):
            api.ensure_running("cluster_id")
