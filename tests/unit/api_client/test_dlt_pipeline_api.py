import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import DltPipelineApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestDltPipelineApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return DltPipelineApi(session, host, 1)

    @pytest.fixture
    def pipeline_id(self):
        return "pipeline_id"

    @pytest.fixture
    def update_id(self):
        return "update_id"

    def test_get_update_error__non_200(self, api, session, pipeline_id, update_id):
        session.get.return_value.status_code = 500
        with pytest.raises(DbtRuntimeError):
            api.get_update_error(pipeline_id, update_id)

    def test_get_update_error__200_no_events(self, api, session, pipeline_id, update_id):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"events": []}
        assert api.get_update_error(pipeline_id, update_id) == ""

    def test_get_update_error__200_no_error_events(self, api, session, pipeline_id, update_id):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "events": [{"event_type": "update_progress", "origin": {"update_id": update_id}}]
        }
        assert api.get_update_error(pipeline_id, update_id) == ""

    def test_get_update_error__200_error_events(self, api, session, pipeline_id, update_id):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "events": [
                {
                    "message": "I failed",
                    "details": {"update_progress": {"state": "FAILED"}},
                    "event_type": "update_progress",
                    "origin": {"update_id": update_id},
                }
            ]
        }
        assert api.get_update_error(pipeline_id, update_id) == "I failed"

    def test_poll_for_completion__non_200(self, api, session, pipeline_id):
        self.assert_non_200_raises_error(lambda: api.poll_for_completion(pipeline_id), session)

    def test_poll_for_completion__200(self, api, session, host, pipeline_id):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"state": "IDLE"}
        api.poll_for_completion(pipeline_id)
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines/{pipeline_id}", json=None, params={}
        )

    def test_poll_for_completion__failed_with_cause(self, api, session, pipeline_id):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "state": "FAILED",
            "pipeline_id": pipeline_id,
            "cause": "I failed",
        }
        with pytest.raises(DbtRuntimeError, match=f"Pipeline {pipeline_id} failed: I failed"):
            api.poll_for_completion(pipeline_id)
