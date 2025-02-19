from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import connections
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

    def test_get_max_idle__no_config(self, creds):
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(), creds)
        assert connections.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__no_matching_compute(self, creds):
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(compute_name="foo"), creds)
        assert connections.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__compute_without_details(self, creds):
        creds.compute = {"foo": {}}
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(compute_name="foo"), creds)
        assert connections.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__creds_but_no_context(self, creds):
        creds.connect_max_idle = 77
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(), creds)
        assert 77 == time

    def test_get_max_idle__matching_compute_no_value(self, creds):
        creds.connect_max_idle = 77
        creds.compute = {"foo": {}}
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(compute_name="foo"), creds)
        assert 77 == time

    def test_get_max_idle__matching_compute(self, creds):
        creds.compute = {"foo": {"connect_max_idle": "88"}}
        creds.connect_max_idle = 77
        time = QueryConfigUtils.get_max_idle_time(QueryContextWrapper(compute_name="foo"), creds)
        assert 88 == time

    def test_get_max_idle__invalid_config(self, creds):
        creds.compute = {"foo": {"connect_max_idle": "bar"}}

        with pytest.raises(DbtRuntimeError) as info:
            QueryConfigUtils.get_max_idle_time(QueryContextWrapper(compute_name="foo"), creds)
        assert (
            "bar is not a valid value for connect_max_idle. Must be a number of seconds."
        ) in str(info.value)
