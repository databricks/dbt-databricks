from unittest.mock import patch

import freezegun
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import JobRunsApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestJobRunsApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return JobRunsApi(session, host, 1, 2)

    def test_submit__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.submit("run_name", {}), session)

    def test_submit__200(self, api, session, host):
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"run_id": "run_id"}
        assert api.submit("run_name", {}) == "run_id"
        session.post.assert_called_once_with(
            f"https://{host}/api/2.1/jobs/runs/submit",
            json={"run_name": "run_name", "tasks": [{}]},
            params=None,
        )

    def test_get_run_info__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.get_run_info("run_id"), session)

    def test_get_run_info__200(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"job_id": 123, "run_id": "run_id"}
        result = api.get_run_info("run_id")
        assert result == {"job_id": 123, "run_id": "run_id"}
        session.get.assert_called_once_with(
            f"https://{host}/api/2.1/jobs/runs/get",
            json=None,
            params={"run_id": "run_id"},
        )

    def test_get_job_id_from_run_id__job_id_exists(self, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"job_id": 123, "run_id": "run_id"}
        assert api.get_job_id_from_run_id("run_id") == "123"

    def test_get_job_id_from_run_id__no_job_id(self, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"run_id": "run_id"}
        with pytest.raises(DbtRuntimeError, match="Could not get job_id from run_id run_id"):
            api.get_job_id_from_run_id("run_id")

    def test_cancel__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.cancel("run_id"), session)

    def test_cancel__200(self, api, session, host):
        session.post.return_value.status_code = 200
        api.cancel("run_id")
        session.post.assert_called_once_with(
            f"https://{host}/api/2.1/jobs/runs/cancel",
            json={"run_id": "run_id"},
            params=None,
        )

    def test_poll_for_completion__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.poll_for_completion("run_id"), session)

    @freezegun.freeze_time("2020-01-01", auto_tick_seconds=3)
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__exceed_timeout(self, _, api):
        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("run_id")

        assert "Python model run timed out" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__error_handling_bailout(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "state": {"life_cycle_state": "INTERNAL_ERROR", "state_message": "error"},
        }

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("run_id")

        assert "Python model run ended in state INTERNAL_ERROR" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__error_handling_task_status(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.side_effect = [
            {
                "state": {"life_cycle_state": "INTERNAL_ERROR", "state_message": "error"},
                "tasks": [{"run_id": "1"}],
            },
            {
                "state": {"life_cycle_state": "INTERNAL_ERROR", "state_message": "error"},
                "tasks": [{"run_id": "1"}],
            },
            {"error": "Fancy exception", "error_trace": "trace"},
        ]

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("run_id")

        assert "Fancy exception" in str(exc.value)
        assert "trace" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__cancelled(self, _, api, session):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "CANCELED",
                "state_message": "cancelled by user",
            }
        }

        with pytest.raises(DbtRuntimeError) as exc:
            api.poll_for_completion("run_id")

        assert "Python model run ended in result_state CANCELED" in str(exc.value)

    @freezegun.freeze_time("2020-01-01")
    @patch("dbt.adapters.databricks.api_client.time.sleep")
    def test_poll_for_completion__200(self, _, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": {"life_cycle_state": "TERMINATED"}}

        api.poll_for_completion("run_id")

        session.get.assert_called_once_with(
            f"https://{host}/api/2.1/jobs/runs/get",
            json=None,
            params={"run_id": "run_id"},
        )
