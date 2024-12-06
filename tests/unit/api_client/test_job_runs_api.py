from unittest.mock import Mock

import pytest
from databricks.sdk.service.jobs import RunResultState, TerminationTypeType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import JobRunsApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestJobRunsApi(ApiTestBase):
    @pytest.fixture
    def api(self, client):
        return JobRunsApi(client, 1)

    @pytest.fixture
    def jobs_client(self, client):
        return client.jobs

    def test_submit__no_run_id(self, api, jobs_client):
        jobs_client.submit_and_wait.return_value = Mock(status=None, run_id=None)
        with pytest.raises(DbtRuntimeError, match="No id returned"):
            api.submit("run_name", {})

    def test_submit__failing_status(self, api, jobs_client):
        jobs_client.submit_and_wait.return_value = Mock(
            status=Mock(termination_details=Mock(type=TerminationTypeType.INTERNAL_ERROR))
        )
        with pytest.raises(DbtRuntimeError, match="Error submitting job run"):
            api.submit("run_name", {})

    def test_submit__success(self, api, jobs_client):
        jobs_client.submit_and_wait.return_value = Mock(
            status=Mock(termination_details=Mock(type=TerminationTypeType.SUCCESS)), run_id=1
        )
        assert api.submit("run_name", {}) == 1

    def test_poll_for_completion__error(self, api, jobs_client):
        jobs_client.wait_get_run_job_terminated_or_skipped.return_value = Mock(
            state=Mock(result_state=RunResultState.FAILED), tasks=None
        )
        with pytest.raises(DbtRuntimeError, match="Python model run ended in state"):
            api.poll_for_completion(1)

    def test_poll_for_completion__success(self, api, jobs_client):
        jobs_client.wait_get_run_job_terminated_or_skipped.return_value = Mock(
            state=Mock(result_state=RunResultState.SUCCESS)
        )
        # Noop, no error
        api.poll_for_completion(1)
