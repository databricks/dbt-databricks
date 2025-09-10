import base64
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import WorkspaceApi


class TestWorkspaceApi:
    @pytest.fixture
    def user_api(self):
        mock = Mock()
        mock.get_folder.return_value = "/user"
        return mock

    @pytest.fixture
    def workspace_client(self):
        mock = Mock()
        return mock

    @pytest.fixture
    def api(self, workspace_client, user_api):
        return WorkspaceApi(workspace_client, user_api)

    def test_create_python_model_dir__exception(self, api, workspace_client):
        workspace_client.workspace.mkdirs.side_effect = Exception("API Error")
        with pytest.raises(Exception, match="Error creating work_dir for python notebooks"):
            api.create_python_model_dir("catalog", "schema")

    def test_create_python_model_dir__success(self, api, workspace_client):
        folder = api.create_python_model_dir("catalog", "schema")
        assert folder == "/user"
        workspace_client.workspace.mkdirs.assert_called_once_with(path="/user")

    def test_upload_notebook__exception(self, api, workspace_client):
        workspace_client.workspace.import_.side_effect = Exception("API Error")
        with pytest.raises(Exception, match="Error creating python notebook"):
            api.upload_notebook("path", "code")

    def test_upload_notebook__success(self, api, workspace_client):
        from databricks.sdk.service.workspace import ImportFormat, Language

        encoded = base64.b64encode(b"code").decode()
        api.upload_notebook("path", "code")
        workspace_client.workspace.import_.assert_called_once_with(
            path="path",
            content=encoded,
            language=Language.PYTHON,
            overwrite=True,
            format=ImportFormat.SOURCE,
        )

    def test_get_object_id__exception(self, api, workspace_client):
        workspace_client.workspace.get_status.side_effect = Exception("API Error")
        with pytest.raises(Exception, match="Error getting workspace object ID"):
            api.get_object_id("path")

    def test_get_object_id__success(self, api, workspace_client):
        mock_status = Mock()
        mock_status.object_id = 12345
        workspace_client.workspace.get_status.return_value = mock_status

        object_id = api.get_object_id("path")
        assert object_id == 12345
        workspace_client.workspace.get_status.assert_called_once_with(path="path")
