from unittest.mock import Mock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import connections
from dbt.adapters.databricks.credentials import DatabricksCredentials


class TestDatabricksConnectionHTTPPath:
    """Test the various cases for determining a specified warehouse."""

    @pytest.fixture(scope="class")
    def err_msg(self):
        return (
            "Compute resource foo does not exist or does not specify http_path,"
            " relation: a_relation"
        )

    @pytest.fixture(scope="class")
    def path(self):
        return "my_http_path"

    @pytest.fixture
    def creds(self, path):
        with patch("dbt.adapters.databricks.credentials.Config"):
            return DatabricksCredentials(http_path=path)

    @pytest.fixture
    def node(self):
        n = Mock()
        n.config = {}
        n.relation_name = "a_relation"
        return n

    def test_get_http_path__empty(self, path, creds):
        assert connections._get_http_path(None, creds) == path

    def test_get_http_path__no_compute(self, node, path, creds):
        assert connections._get_http_path(node, creds) == path

    def test_get_http_path__missing_compute(self, node, creds, err_msg):
        node.config["databricks_compute"] = "foo"
        with pytest.raises(DbtRuntimeError) as exc:
            connections._get_http_path(node, creds)

        assert err_msg in str(exc.value)

    def test_get_http_path__empty_compute(self, node, creds, err_msg):
        node.config["databricks_compute"] = "foo"
        creds.compute = {"foo": {}}
        with pytest.raises(DbtRuntimeError) as exc:
            connections._get_http_path(node, creds)

        assert err_msg in str(exc.value)

    def test_get_http_path__matching_compute(self, node, creds):
        node.config["databricks_compute"] = "foo"
        creds.compute = {"foo": {"http_path": "alternate_path"}}
        assert "alternate_path" == connections._get_http_path(node, creds)
