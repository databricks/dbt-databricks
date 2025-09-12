import pytest

from dbt.adapters.databricks.api_client import SharedFolderApi


class TestSharedFolderApi:
    @pytest.fixture
    def api(self):
        return SharedFolderApi()

    def test_get_folder(self, api):
        # Test that it returns the expected shared folder path
        folder = api.get_folder("catalog", "schema")
        assert folder == "/Shared/dbt_python_models/schema/"

    def test_get_folder_ignores_catalog(self, api):
        # Test that catalog parameter is ignored (as indicated by the _ parameter)
        folder1 = api.get_folder("catalog1", "schema")
        folder2 = api.get_folder("catalog2", "schema")
        assert folder1 == folder2 == "/Shared/dbt_python_models/schema/"

    def test_get_folder_different_schemas(self, api):
        # Test that different schemas produce different folder paths
        folder1 = api.get_folder("catalog", "schema1")
        folder2 = api.get_folder("catalog", "schema2")
        assert folder1 == "/Shared/dbt_python_models/schema1/"
        assert folder2 == "/Shared/dbt_python_models/schema2/"
        assert folder1 != folder2
