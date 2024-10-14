import pytest
from dbt.adapters.databricks.api_client import UserFolderApi
from dbt.adapters.databricks.api_client import CurrUserApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestUserFolderApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        user_api = CurrUserApi(session, host)
        return UserFolderApi(session, host, user_api)

    def test_get_folder__already_set(self, api):
        api.user_api._user = "me"
        assert "/Users/me/dbt_python_models/catalog/schema/" == api.get_folder("catalog", "schema")

    def test_get_folder__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.get_folder("catalog", "schema"), session)

    def test_get_folder__200(self, api, session, host):
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"userName": "me@gmail.com"}
        folder = api.get_folder("catalog", "schema")
        assert folder == "/Users/me@gmail.com/dbt_python_models/catalog/schema/"
        assert api.user_api._user == "me@gmail.com"
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/preview/scim/v2/Me", json=None, params=None
        )
