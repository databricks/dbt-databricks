from unittest.mock import Mock

import pytest
from databricks.sdk.service.pipelines import PipelineState
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import DltPipelineApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestDltPipelineApi(ApiTestBase):
    @pytest.fixture
    def api(self, client):
        return DltPipelineApi(client)

    @pytest.fixture
    def pipeline_client(self, client):
        return client.pipelines

    @pytest.fixture
    def pipeline_id(self):
        return "pipeline_id"

    @pytest.fixture
    def update_id(self):
        return "update_id"

    def test_get_update_error__no_events(self, api, pipeline_client, pipeline_id, update_id):
        pipeline_client.list_pipeline_events.return_value = []
        assert api._get_update_error(pipeline_id, update_id) == ""

    def test_get_update_error__no_error_events(self, api, pipeline_client, pipeline_id, update_id):
        pipeline_client.list_pipeline_events.return_value = [
            Mock(event_type="update_progress", origin=Mock(update_id=update_id), error=None)
        ]
        assert api._get_update_error(pipeline_id, update_id) == ""

    def test_get_update_error__error_events(self, api, pipeline_client, pipeline_id, update_id):
        pipeline_client.list_pipeline_events.return_value = [
            Mock(
                event_type="update_progress",
                origin=Mock(update_id=update_id),
                error=Mock(exceptions=[Mock(message="I failed")]),
            )
        ]
        assert api._get_update_error(pipeline_id, update_id) == "I failed"

    def test_poll_for_completion__error_with_cause(self, api, pipeline_client, pipeline_id):
        pipeline_client.wait_get_pipeline_idle.return_value = Mock(
            state=PipelineState.FAILED, cause="I failed"
        )
        with pytest.raises(DbtRuntimeError, match=f"Pipeline {pipeline_id} failed: I failed"):
            api.poll_for_completion(pipeline_id)

    def test_poll_for_completion__error_without_cause(
        self, api, pipeline_client, pipeline_id, update_id
    ):
        pipeline_client.wait_get_pipeline_idle.return_value = Mock(
            state=PipelineState.FAILED, latest_updates=[update_id], cause=None
        )
        pipeline_client.list_pipeline_events.return_value = [
            Mock(
                event_type="update_progress",
                origin=Mock(update_id=update_id),
                error=Mock(exceptions=[Mock(message="other fail")]),
            )
        ]
        with pytest.raises(DbtRuntimeError, match=f"Pipeline {pipeline_id} failed: other fail"):
            api.poll_for_completion(pipeline_id)

    def test_poll_for_completion__success(self, api, pipeline_client, pipeline_id, update_id):
        pipeline_client.wait_get_pipeline_idle.return_value = Mock(
            state=PipelineState.IDLE, latest_updates=[update_id]
        )
        # Noop, no error
        api.poll_for_completion(pipeline_id)
