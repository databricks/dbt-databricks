from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import CurrUserApi, UserFolderApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestUserFolderApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        # Mock WorkspaceClient for CurrUserApi
        workspace_client = Mock()
        user_api = CurrUserApi(workspace_client)
        return UserFolderApi(session, host, user_api)

    def test_get_folder__already_set(self, api):
        api.user_api._user = "me"
        assert "/Users/me/dbt_python_models/catalog/schema/" == api.get_folder("catalog", "schema")

    def test_get_folder__non_200(self, api, session):
        # Make the WorkspaceClient raise an exception
        api.user_api.workspace_client.current_user.me.side_effect = Exception("API Error")
        self.assert_non_200_raises_error(lambda: api.get_folder("catalog", "schema"), session)

    def test_get_folder__200(self, api, session, host):
        # Mock the WorkspaceClient's current_user.me() method
        mock_user = Mock()
        mock_user.user_name = "me@gmail.com"
        api.user_api.workspace_client.current_user.me.return_value = mock_user

        folder = api.get_folder("catalog", "schema")
        assert folder == "/Users/me@gmail.com/dbt_python_models/catalog/schema/"
        assert api.user_api._user == "me@gmail.com"
        api.user_api.workspace_client.current_user.me.assert_called_once()
