import base64
import json
import os
import sys
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from databricks.sql.client import Cursor
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.handle import (
    CursorWrapper,
    DatabricksAdapterResponse,
    DatabricksHandle,
    SqlUtils,
    _get_job_run_context,
)


def _build_session_headers(
    job_id: str = "222",
    task_run_id: str = "333",
    job_run_id: str = "111",
    include_source: bool = True,
    source_override: object = None,
) -> str:
    """Build a realistic DBT_DATABRICKS_HTTP_SESSION_HEADERS JSON string."""
    payload: dict[str, object] = {
        "X-Databricks-Dbsql-Attribution-Flags": {
            "dbsqlTriggerSource": "jobsScheduler",
            "jobId": job_id,
            "dbsqlTriggerExecutionType": "manual",
            "dbsqlTriggerAssetType": "dbt",
            "runId": task_run_id,
        },
        "X-Databricks-Dbsql-Job-Id": job_id,
        "X-Databricks-Dbsql-Run-Id": task_run_id,
    }
    if include_source:
        if source_override is not None:
            payload["X-Databricks-Sql-Query-Source"] = source_override
        else:
            inner = json.dumps(
                {
                    "job_run_id": job_run_id,
                    "job_id": job_id,
                    "run_id": task_run_id,
                    "scheduled": False,
                    "job_type": "EPHEMERAL",
                }
            )
            payload["X-Databricks-Sql-Query-Source"] = base64.b64encode(inner.encode()).decode()
    return json.dumps(payload)


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

    @patch.dict(os.environ, {}, clear=True)
    def test_get_response__no_query_id(self, cursor):
        cursor.query_id = None
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert isinstance(response, DatabricksAdapterResponse)
        assert response.query_id == "N/A"
        assert response.job_id is None
        assert response.job_run_id is None
        assert response.task_run_id is None

    @patch.dict(os.environ, {}, clear=True)
    def test_get_response__with_query_id(self, cursor):
        cursor.query_id = "id"
        wrapper = CursorWrapper(cursor)
        response = wrapper.get_response()
        assert isinstance(response, DatabricksAdapterResponse)
        assert response.query_id == "id"

    def test_get_response__with_job_context(self, cursor):
        cursor.query_id = "qid"
        wrapper = CursorWrapper(cursor)
        with patch.dict(
            os.environ,
            {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers()},
        ):
            response = wrapper.get_response()
        assert response.job_id == "222"
        assert response.job_run_id == "111"
        assert response.task_run_id == "333"
        assert response.query_id == "qid"

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
    @patch.dict(os.environ, {}, clear=True)
    def test_env_absent(self):
        assert _get_job_run_context() == {
            "job_id": None,
            "job_run_id": None,
            "task_run_id": None,
        }

    @patch.dict(os.environ, {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": ""})
    def test_env_empty_string(self):
        assert _get_job_run_context() == {
            "job_id": None,
            "job_run_id": None,
            "task_run_id": None,
        }

    def test_real_headers_full(self):
        with patch.dict(
            os.environ,
            {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers()},
        ):
            assert _get_job_run_context() == {
                "job_id": "222",
                "job_run_id": "111",
                "task_run_id": "333",
            }

    def test_headers_without_query_source(self):
        """job_run_id is None when X-Databricks-Sql-Query-Source is absent."""
        with patch.dict(
            os.environ,
            {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers(include_source=False)},
        ):
            ctx = _get_job_run_context()
        assert ctx == {"job_id": "222", "job_run_id": None, "task_run_id": "333"}

    @patch.dict(os.environ, {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": "not-json{"}, clear=False)
    def test_malformed_json(self):
        assert _get_job_run_context() == {
            "job_id": None,
            "job_run_id": None,
            "task_run_id": None,
        }

    def test_malformed_base64_source(self):
        """Bad base64 in X-Databricks-Sql-Query-Source leaves job_run_id None
        but the directly-readable job_id and task_run_id still resolve."""
        with patch.dict(
            os.environ,
            {
                "DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers(
                    source_override="!!!not-base64!!!"
                )
            },
        ):
            ctx = _get_job_run_context()
        assert ctx == {"job_id": "222", "job_run_id": None, "task_run_id": "333"}

    def test_base64_decodes_to_non_json(self):
        garbage_b64 = base64.b64encode(b"not json").decode()
        with patch.dict(
            os.environ,
            {
                "DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers(
                    source_override=garbage_b64
                )
            },
        ):
            ctx = _get_job_run_context()
        assert ctx == {"job_id": "222", "job_run_id": None, "task_run_id": "333"}


class TestDatabricksAdapterResponse:
    @patch.dict(os.environ, {}, clear=True)
    def test_from_cursor__no_context(self):
        cursor = Mock()
        cursor.query_id = "q1"
        resp = DatabricksAdapterResponse.from_cursor(cursor)
        assert resp.query_id == "q1"
        assert resp.job_id is None
        assert resp.job_run_id is None
        assert resp.task_run_id is None

    def test_from_cursor__with_context(self):
        cursor = Mock()
        cursor.query_id = "qid"
        with patch.dict(
            os.environ,
            {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers()},
        ):
            resp = DatabricksAdapterResponse.from_cursor(cursor)
        assert resp.job_id == "222"
        assert resp.job_run_id == "111"
        assert resp.task_run_id == "333"
