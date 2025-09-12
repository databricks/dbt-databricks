from unittest.mock import Mock

import pytest
from databricks.sdk.service.iam import AccessControlRequest, ObjectPermissions
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import JobPermissionsApi


class TestJobPermissionsApi:
    @pytest.fixture
    def workspace_client(self):
        return Mock()

    @pytest.fixture
    def api(self, workspace_client):
        return JobPermissionsApi(workspace_client)

    def test_put__exception(self, api, workspace_client):
        workspace_client.permissions.set.side_effect = Exception("API Error")
        access_control_list = [{"user_name": "test@example.com", "permission_level": "CAN_MANAGE"}]

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.put("123", access_control_list)

        assert "Error updating Databricks workflow" in str(exc_info.value)

    def test_put__success(self, api, workspace_client):
        mock_result = Mock(spec=ObjectPermissions)
        workspace_client.permissions.set.return_value = mock_result
        access_control_list = [{"user_name": "test@example.com", "permission_level": "CAN_MANAGE"}]

        api.put("123", access_control_list)

        workspace_client.permissions.set.assert_called_once_with(
            request_object_type="jobs",
            request_object_id="123",
            access_control_list=[AccessControlRequest.from_dict(access_control_list[0])],
        )

    def test_patch__exception(self, api, workspace_client):
        workspace_client.permissions.update.side_effect = Exception("API Error")
        access_control_list = [{"user_name": "test@example.com", "permission_level": "CAN_VIEW"}]

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.patch("123", access_control_list)

        assert "Error updating Databricks workflow" in str(exc_info.value)

    def test_patch__success(self, api, workspace_client):
        mock_result = Mock(spec=ObjectPermissions)
        workspace_client.permissions.update.return_value = mock_result
        access_control_list = [{"user_name": "test@example.com", "permission_level": "CAN_VIEW"}]

        api.patch("123", access_control_list)

        workspace_client.permissions.update.assert_called_once_with(
            request_object_type="jobs",
            request_object_id="123",
            access_control_list=[AccessControlRequest.from_dict(access_control_list[0])],
        )

    def test_get__exception(self, api, workspace_client):
        workspace_client.permissions.get.side_effect = Exception("API Error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            api.get("123")

        assert "Error fetching Databricks workflow permissions" in str(exc_info.value)

    def test_get__success(self, api, workspace_client):
        mock_result = Mock(spec=ObjectPermissions)
        expected_dict = {"object_id": "123", "object_type": "jobs", "access_control_list": []}
        mock_result.as_dict.return_value = expected_dict
        workspace_client.permissions.get.return_value = mock_result

        result = api.get("123")

        assert result == expected_dict
        workspace_client.permissions.get.assert_called_once_with(
            request_object_type="jobs", request_object_id="123"
        )
