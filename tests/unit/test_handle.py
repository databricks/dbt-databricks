import os
import sys
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from databricks.sql.client import Cursor
from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.handle import (
    CursorWrapper,
    DatabricksAdapterResponse,
    DatabricksHandle,
    SqlUtils,
    _get_job_run_context,
)


class TestSqlUtils:
    @pytest.mark.parametrize(
        "bindings, expected", [(None, None), ([1], [1]), ([1, Decimal(0.73)], [1, 0.73])]
    )
    def test_translate_bindings(self, bindings, expected):
        assert SqlUtils.translate_bindings(bindings) == expected

    @pytest.mark.parametrize(
        "sql, expected", [(" select 1; ", "select 1"), ("select 1", "select 1")]
    )
    def test_clean_sql(self, sql, expected):
        assert SqlUtils.clean_sql(sql) == expected

    @pytest.mark.parametrize("result, expected", [("14.x", (14, sys.maxsize)), ("12.1", (12, 1))])
    def test_extract_dbr_version(self, result, expected):
        assert SqlUtils.extract_dbr_version(result) == expected

    def test_extract_dbr_version__invalid(self):
        with pytest.raises(DbtRuntimeError):
            SqlUtils.extract_dbr_version("foo")


class TestCursorWrapper:
    @pytest.fixture
    def cursor(self):
        return Mock()

    def test_description(self, cursor):
        cursor.description = [("foo", "bar")]
        wrapper = CursorWrapper(cursor)
        assert wrapper.description == [("foo", "bar")]

    def test_cancel__closed(self, cursor):
        wrapper = CursorWrapper(cursor)
        wrapper.open = False
        wrapper.cancel()
        cursor.cancel.assert_not_called()

    def test_cancel__open_no_result_set(self, cursor):
        wrapper = CursorWrapper(cursor)
        cursor.active_result_set = None
        wrapper.cancel()
        assert wrapper.open is False

    def test_cancel__open_with_result_set(self, cursor):
        wrapper = CursorWrapper(cursor)
        wrapper.cancel()
        cursor.cancel.assert_called_once()

    def test_cancel__error_cancelling(self, cursor):
        cursor.cancel.side_effect = Exception("foo")
        wrapper = CursorWrapper(cursor)
        wrapper.cancel()
        cursor.cancel.assert_called_once()

    def test_closed__closed(self, cursor):
        wrapper = CursorWrapper(cursor)
        wrapper.open = False
        wrapper.close()
        cursor.close.assert_not_called()

    def test_closed__open(self, cursor):
        wrapper = CursorWrapper(cursor)
        cursor.active_result_set = None
        wrapper.close()
        assert wrapper.open is False

    def test_close__error_closing(self, cursor):
        cursor.close.side_effect = Exception("foo")
        wrapper = CursorWrapper(cursor)
        wrapper.close()
        cursor.close.assert_called_once()

    def test_fetchone(self, cursor):
        cursor.fetchone.return_value = [("foo", "bar")]
        wrapper = CursorWrapper(cursor)
        assert wrapper.fetchone() == [("foo", "bar")]

    def test_fetchall(self, cursor):
        cursor.fetchall.return_value = [("foo", "bar")]
        wrapper = CursorWrapper(cursor)
        assert wrapper.fetchall() == [("foo", "bar")]

    def test_fetchmany(self, cursor):
        cursor.fetchmany.return_value = [("foo", "bar")]
        wrapper = CursorWrapper(cursor)
        assert wrapper.fetchmany(1) == [("foo", "bar")]

    def test_get_response__no_query_id(self, cursor):
        cursor.query_id = None
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert response._message == "OK"
        assert response.query_id == "N/A"

    def test_get_response__with_query_id(self, cursor):
        cursor.query_id = "id"
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert response._message == "OK"
        assert response.query_id == "id"

    @patch.dict(
        os.environ,
        {"DATABRICKS_JOB_ID": "123", "DATABRICKS_RUN_ID": "456", "DATABRICKS_TASK_KEY": "my_task"},
    )
    def test_get_response__with_job_context(self, cursor):
        cursor.query_id = "qid"
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert isinstance(response, DatabricksAdapterResponse)
        assert response.job_id == "123"
        assert response.run_id == "456"
        assert response.task_key == "my_task"
        assert response.query_id == "qid"

    @patch.dict(os.environ, {}, clear=True)
    def test_get_response__without_job_context(self, cursor):
        cursor.query_id = "qid"
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert isinstance(response, DatabricksAdapterResponse)
        assert response.job_id is None
        assert response.run_id is None
        assert response.task_key is None

    def test_with__no_exception(self, cursor):
        with CursorWrapper(cursor) as c:
            c.fetchone()
        cursor.fetchone.assert_called_once()
        cursor.close.assert_called_once()

    def test_with__exception(self, cursor):
        cursor.fetchone.side_effect = Exception("foo")
        with pytest.raises(Exception, match="foo"):
            with CursorWrapper(cursor) as c:
                c.fetchone()
        cursor.fetchone.assert_called_once()
        cursor.close.assert_called_once()


class TestDatabricksHandle:
    @pytest.fixture
    def conn(self):
        return Mock()

    @pytest.fixture
    def cursor(self):
        return Mock()

    def test_safe_execute__closed(self, conn):
        handle = DatabricksHandle(conn, True)
        handle.open = False
        with pytest.raises(DbtRuntimeError, match="Attempting to execute on a closed connection"):
            handle._safe_execute(Mock())

    def test_safe_execute__with_cursor(self, conn, cursor):
        new_cursor = Mock()

        def f(_: Cursor) -> Cursor:
            return new_cursor

        handle = DatabricksHandle(conn, True)
        handle._cursor = cursor
        assert handle._safe_execute(f)._cursor == new_cursor
        assert handle._cursor._cursor == new_cursor
        cursor.close.assert_called_once()

    def test_safe_execute__without_cursor(self, conn):
        new_cursor = Mock()

        def f(_: Cursor) -> Cursor:
            return new_cursor

        handle = DatabricksHandle(conn, True)
        assert handle._safe_execute(f)._cursor == new_cursor
        assert handle._cursor._cursor == new_cursor

    def test_cancel__closed(self, conn):
        handle = DatabricksHandle(conn, True)
        handle.open = False
        handle.cancel()
        conn.close.assert_not_called()

    def test_cancel__open_no_cursor(self, conn):
        handle = DatabricksHandle(conn, True)
        handle.cancel()
        conn.close.assert_called_once()

    def test_cancel__open_cursor(self, conn, cursor):
        handle = DatabricksHandle(conn, True)
        handle._cursor = cursor
        handle.cancel()
        cursor.cancel.assert_called_once()
        conn.close.assert_called_once()

    def test_cancel__open_raising_exception(self, conn):
        conn.close.side_effect = Exception("foo")
        handle = DatabricksHandle(conn, True)
        handle.cancel()
        conn.close.assert_called_once()

    def test_close__closed(self, conn):
        handle = DatabricksHandle(conn, True)
        handle.open = False
        handle.close()
        conn.close.assert_not_called()

    def test_close__open_no_cursor(self, conn):
        handle = DatabricksHandle(conn, True)
        handle.close()
        conn.close.assert_called_once()

    def test_close__open_cursor(self, conn, cursor):
        handle = DatabricksHandle(conn, True)
        handle._cursor = cursor
        handle.close()
        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_close__open_raising_exception(self, conn, cursor):
        conn.close.side_effect = Exception("foo")
        handle = DatabricksHandle(conn, True)
        handle._cursor = cursor
        handle.close()
        cursor.close.assert_called_once()
        conn.close.assert_called_once()


class TestGetJobRunContext:
    @patch.dict(
        os.environ,
        {"DATABRICKS_JOB_ID": "111", "DATABRICKS_RUN_ID": "222", "DATABRICKS_TASK_KEY": "etl"},
    )
    def test_all_vars_set(self):
        ctx = _get_job_run_context()
        assert ctx == {"job_id": "111", "run_id": "222", "task_key": "etl"}

    @patch.dict(os.environ, {"DATABRICKS_JOB_ID": "111"}, clear=True)
    def test_partial_vars(self):
        ctx = _get_job_run_context()
        assert ctx["job_id"] == "111"
        assert ctx["run_id"] is None
        assert ctx["task_key"] is None

    @patch.dict(os.environ, {}, clear=True)
    def test_no_vars(self):
        ctx = _get_job_run_context()
        assert ctx == {"job_id": None, "run_id": None, "task_key": None}


class TestDatabricksAdapterResponse:
    def test_from_cursor__with_all_context(self):
        cursor = Mock()
        cursor.query_id = "q1"
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_JOB_ID": "10",
                "DATABRICKS_RUN_ID": "20",
                "DATABRICKS_TASK_KEY": "transform",
            },
        ):
            resp = DatabricksAdapterResponse.from_cursor(cursor)
        assert resp._message == "OK"
        assert resp.query_id == "q1"
        assert resp.job_id == "10"
        assert resp.run_id == "20"
        assert resp.task_key == "transform"

    def test_from_cursor__no_context(self):
        cursor = Mock()
        cursor.query_id = "q2"
        with patch.dict(os.environ, {}, clear=True):
            resp = DatabricksAdapterResponse.from_cursor(cursor)
        assert resp._message == "OK"
        assert resp.query_id == "q2"
        assert resp.job_id is None
        assert resp.run_id is None
        assert resp.task_key is None

    def test_from_cursor__no_query_id(self):
        cursor = Mock()
        cursor.query_id = None
        with patch.dict(os.environ, {}, clear=True):
            resp = DatabricksAdapterResponse.from_cursor(cursor)
        assert resp.query_id == "N/A"

    def test_str_representation(self):
        resp = DatabricksAdapterResponse(_message="OK", query_id="q1", job_id="10")
        assert str(resp) == "OK"

    def test_is_adapter_response_subclass(self):
        resp = DatabricksAdapterResponse(_message="OK")
        assert isinstance(resp, AdapterResponse)
