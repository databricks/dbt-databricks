from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import CurrUserApi, UserFolderApi


class TestUserFolderApi:
    @pytest.fixture
    def api(self):
        # Mock WorkspaceClient for CurrUserApi
        workspace_client = Mock()
        user_api = CurrUserApi(workspace_client)
        return UserFolderApi(user_api)

    def test_get_folder__already_set(self, api):
        api.user_api._user = "me"
        assert "/Users/me/dbt_python_models/catalog/schema/" == api.get_folder("catalog", "schema")

    def test_get_folder__non_200(self, api):
        # Make the WorkspaceClient raise an exception
        api.user_api.workspace_client.current_user.me.side_effect = Exception("API Error")
        with pytest.raises(Exception, match="API Error"):
            api.get_folder("catalog", "schema")

    def test_get_folder__200(self, api):
        # Mock the WorkspaceClient's current_user.me() method
        mock_user = Mock()
        mock_user.user_name = "me@gmail.com"
        api.user_api.workspace_client.current_user.me.return_value = mock_user

        folder = api.get_folder("catalog", "schema")
        assert folder == "/Users/me@gmail.com/dbt_python_models/catalog/schema/"
        assert api.user_api._user == "me@gmail.com"
        api.user_api.workspace_client.current_user.me.assert_called_once()
