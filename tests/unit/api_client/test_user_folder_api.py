from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import UserFolderApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestUserFolderApi(ApiTestBase):
    @pytest.fixture
    def user_api(self):
        return Mock()

    @pytest.fixture
    def api(self, user_api):
        return UserFolderApi(user_api)

    def test_get_folder__success(self, api, user_api):
        user_api.get_username.return_value = "me@gmail.com"
        folder = api.get_folder("catalog", "schema")
        assert folder == "/Users/me@gmail.com/dbt_python_models/catalog/schema"
