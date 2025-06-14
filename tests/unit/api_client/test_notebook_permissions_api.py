import urllib.parse
from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import NotebookPermissionsApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestNotebookPermissionsApi(ApiTestBase):
    @pytest.fixture
    def workspace_api(self):
        mock = Mock()
        mock.get_object_id.return_value = "12345"
        return mock

    @pytest.fixture
    def api(self, session, host, workspace_api):
        api = NotebookPermissionsApi(session, host, workspace_api)
        return api

    @pytest.fixture
    def notebook_path(self):
        return "/Users/me/dbt_python_models/my_notebook"

    @pytest.fixture
    def access_control_list(self):
        return [
            {"user_name": "owner", "permission_level": "IS_OWNER"},
            {"group_name": "data-team", "permission_level": "CAN_VIEW"},
        ]

    def test_put__non_200(self, api, session, notebook_path, access_control_list):
        session.put.return_value.status_code = 500
        with pytest.raises(DbtRuntimeError):
            api.put(notebook_path, access_control_list)
        encoded_path = urllib.parse.quote("12345")
        session.put.assert_called_once_with(
            f"https://host/api/2.0/permissions/notebooks/{encoded_path}",
            json={"access_control_list": access_control_list},
            params=None,
        )

    def test_put__200(self, api, session, host, notebook_path, access_control_list):
        session.put.return_value.status_code = 200
        session.put.return_value.json.return_value = {"access_control_list": access_control_list}
        api.put(notebook_path, access_control_list)
        encoded_path = urllib.parse.quote("12345")
        session.put.assert_called_once_with(
            f"https://host/api/2.0/permissions/notebooks/{encoded_path}",
            json={"access_control_list": access_control_list},
            params=None,
        )
