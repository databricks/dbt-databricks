from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import WorkspaceApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestWorkspaceApi(ApiTestBase):
    @pytest.fixture
    def folder_api(self):
        return Mock()

    @pytest.fixture
    def workspace_api(self, client):
        return client.workspace

    @pytest.fixture
    def api(self, client, folder_api):
        return WorkspaceApi(client, folder_api)

    def test_create_python_model_dir__success(self, api, folder_api, workspace_api):
        folder_api.get_folder.return_value = "path"
        folder = api.create_python_model_dir("catalog", "schema")
        assert folder == "path"
        workspace_api.mkdirs.assert_called_once_with("path")
