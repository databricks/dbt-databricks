import base64
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.api_client import WorkspaceApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestWorkspaceApi(ApiTestBase):
    @pytest.fixture
    def user_api(self):
        mock = Mock()
        mock.get_folder.return_value = "/user"
        return mock

    @pytest.fixture
    def api(self, session, host, user_api):
        return WorkspaceApi(session, host, user_api)

    def test_create_python_model_dir__non_200(self, api, session):
        self.assert_non_200_raises_error(
            lambda: api.create_python_model_dir("catalog", "schema"), session
        )

    def test_create_python_model_dir__200(self, api, session, host):
        session.post.return_value.status_code = 200
        folder = api.create_python_model_dir("catalog", "schema")
        assert folder == "/user"
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/workspace/mkdirs", json={"path": folder}, params=None
        )

    def test_upload_notebook__non_200(self, api, session):
        self.assert_non_200_raises_error(lambda: api.upload_notebook("path", "code"), session)

    def test_upload_notebook__200(self, api, session, host):
        session.post.return_value.status_code = 200
        encoded = base64.b64encode(b"code").decode()
        api.upload_notebook("path", "code")
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/workspace/import",
            json={
                "path": "path",
                "content": encoded,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
            params=None,
        )
