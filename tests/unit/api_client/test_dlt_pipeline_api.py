from datetime import timedelta
from unittest.mock import Mock

import pytest
from databricks.sdk.service.pipelines import PipelineState
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import DltPipelineApi


class TestDltPipelineApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return DltPipelineApi(workspace_client, 1)

    @pytest.fixture
    def pipeline_id(self):
        return "pipeline_id"

    @pytest.fixture
    def update_id(self):
        return "update_id"

    def test_poll_for_completion__success(self, api, workspace_client, pipeline_id):
        workspace_client.pipelines.wait_get_pipeline_idle.return_value = None

        api.poll_for_completion(pipeline_id)

        workspace_client.pipelines.wait_get_pipeline_idle.assert_called_once_with(
            pipeline_id=pipeline_id, timeout=timedelta(hours=1)
        )

    def test_poll_for_completion__failed_with_detailed_error(
        self, api, workspace_client, pipeline_id, update_id
    ):
        # Mock the wait method to raise an exception
        workspace_client.pipelines.wait_get_pipeline_idle.side_effect = Exception("Timeout")

        # Mock pipeline get response with failed state
        mock_pipeline = Mock()
        mock_pipeline.state = PipelineState.FAILED
        mock_update = Mock()
        mock_update.update_id = update_id
        mock_pipeline.latest_updates = [mock_update]
        workspace_client.pipelines.get.return_value = mock_pipeline

        # Mock events response with error message
        mock_event = Mock()
        mock_event.event_type = "update_progress"
        mock_event.origin = Mock()
        mock_event.origin.update_id = update_id
        mock_event.error = None
        mock_event.message = "Pipeline execution failed"
        workspace_client.pipelines.list_pipeline_events.return_value = [mock_event]

        with pytest.raises(
            DbtRuntimeError, match=f"Pipeline {pipeline_id} failed: Pipeline execution failed"
        ):
            api.poll_for_completion(pipeline_id)

    def test_poll_for_completion__failed_no_detailed_error(
        self, api, workspace_client, pipeline_id
    ):
        workspace_client.pipelines.wait_get_pipeline_idle.side_effect = Exception("Timeout")
        workspace_client.pipelines.get.side_effect = Exception("Cannot get pipeline")

        with pytest.raises(DbtRuntimeError, match=f"Pipeline {pipeline_id} failed: Timeout"):
            api.poll_for_completion(pipeline_id)

    def test_get_update_error__success_with_error_events(
        self, api, workspace_client, pipeline_id, update_id
    ):
        mock_event = Mock()
        mock_event.event_type = "update_progress"
        mock_event.origin = Mock()
        mock_event.origin.update_id = update_id
        mock_event.error = None
        mock_event.message = "I failed"
        workspace_client.pipelines.list_pipeline_events.return_value = [mock_event]

        result = api.get_update_error(pipeline_id, update_id)

        assert result == "I failed"
        workspace_client.pipelines.list_pipeline_events.assert_called_once_with(
            pipeline_id=pipeline_id
        )

    def test_get_update_error__no_error_events(self, api, workspace_client, pipeline_id, update_id):
        mock_event = Mock()
        mock_event.event_type = "update_progress"
        mock_event.origin = Mock()
        mock_event.origin.update_id = update_id
        mock_event.error = None
        mock_event.message = "I succeeded"  # No "error" in message
        workspace_client.pipelines.list_pipeline_events.return_value = [mock_event]

        result = api.get_update_error(pipeline_id, update_id)

        assert result == ""

    def test_get_update_error__no_events(self, api, workspace_client, pipeline_id, update_id):
        workspace_client.pipelines.list_pipeline_events.return_value = []

        result = api.get_update_error(pipeline_id, update_id)

        assert result == ""

    def test_get_update_error__exception(self, api, workspace_client, pipeline_id, update_id):
        workspace_client.pipelines.list_pipeline_events.side_effect = Exception("SDK error")

        with pytest.raises(DbtRuntimeError, match="Error getting pipeline event info"):
            api.get_update_error(pipeline_id, update_id)
