from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.connections import QueryConfigUtils, QueryContextWrapper


class TestQueryConfigUtils:
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
        return Mock(http_path=path, compute={}, connect_max_idle=None)

    def test_get_http_path__empty(self, path, creds):
        assert QueryConfigUtils.get_http_path(QueryContextWrapper(), creds) == path

    def test_get_http_path__no_compute(self, path, creds):
        assert (
            QueryConfigUtils.get_http_path(QueryContextWrapper(relation_name="a_relation"), creds)
            == path
        )

    def test_get_http_path__missing_compute(self, creds, err_msg):
        context = QueryContextWrapper(compute_name="foo", relation_name="a_relation")
        with pytest.raises(DbtRuntimeError) as exc:
            QueryConfigUtils.get_http_path(context, creds)

        assert err_msg in str(exc.value)

    def test_get_http_path__empty_compute(self, creds, err_msg):
        context = QueryContextWrapper(compute_name="foo", relation_name="a_relation")
        creds.compute = {"foo": {}}
        with pytest.raises(DbtRuntimeError) as exc:
            QueryConfigUtils.get_http_path(context, creds)

        assert err_msg in str(exc.value)

    def test_get_http_path__matching_compute(self, creds):
        context = QueryContextWrapper(compute_name="foo", relation_name="a_relation")
        creds.compute = {"foo": {"http_path": "alternate_path"}}
        assert "alternate_path" == QueryConfigUtils.get_http_path(context, creds)
