from unittest.mock import Mock

import pytest
from databricks.sdk.service.iam import AccessControlRequest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import NotebookPermissionsApi


class TestNotebookPermissionsApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def workspace_api(self):
        mock = Mock()
        mock.get_object_id.return_value = "12345"
        return mock

    @pytest.fixture
    def api(self, workspace_client, workspace_api):
        return NotebookPermissionsApi(workspace_client, workspace_api)

    @pytest.fixture
    def notebook_path(self):
        return "/Users/me/dbt_python_models/my_notebook"

    @pytest.fixture
    def access_control_list(self):
        return [
            {"user_name": "owner", "permission_level": "IS_OWNER"},
            {"group_name": "data-team", "permission_level": "CAN_VIEW"},
        ]

    def test_put__success(
        self, api, workspace_client, workspace_api, notebook_path, access_control_list
    ):
        workspace_client.permissions.set.return_value = None

        api.put(notebook_path, access_control_list)

        workspace_api.get_object_id.assert_called_once_with(notebook_path)
        workspace_client.permissions.set.assert_called_once_with(
            request_object_type="notebooks",
            request_object_id="12345",
            access_control_list=[
                AccessControlRequest.from_dict(
                    {"user_name": "owner", "permission_level": "IS_OWNER"}
                ),
                AccessControlRequest.from_dict(
                    {"group_name": "data-team", "permission_level": "CAN_VIEW"}
                ),
            ],
        )

    def test_put__exception(
        self, api, workspace_client, workspace_api, notebook_path, access_control_list
    ):
        workspace_client.permissions.set.side_effect = Exception("SDK error")

        with pytest.raises(DbtRuntimeError, match="Error updating Databricks notebook permissions"):
            api.put(notebook_path, access_control_list)

        workspace_api.get_object_id.assert_called_once_with(notebook_path)

    def test_get__success(self, api, workspace_client, workspace_api, notebook_path):
        mock_response = Mock()
        mock_response.as_dict.return_value = {
            "access_control_list": [{"user_name": "test", "permission_level": "CAN_VIEW"}]
        }
        workspace_client.permissions.get.return_value = mock_response

        result = api.get(notebook_path)

        workspace_api.get_object_id.assert_called_once_with(notebook_path)
        workspace_client.permissions.get.assert_called_once_with(
            request_object_type="notebooks",
            request_object_id="12345",
        )
        assert result == {
            "access_control_list": [{"user_name": "test", "permission_level": "CAN_VIEW"}]
        }

    def test_get__exception(self, api, workspace_client, workspace_api, notebook_path):
        workspace_client.permissions.get.side_effect = Exception("SDK error")

        with pytest.raises(DbtRuntimeError, match="Error fetching Databricks notebook permissions"):
            api.get(notebook_path)

        workspace_api.get_object_id.assert_called_once_with(notebook_path)
