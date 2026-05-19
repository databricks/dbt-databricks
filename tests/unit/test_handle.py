import sys
from decimal import Decimal
from unittest.mock import Mock

import pytest
from databricks.sql.client import Cursor
from databricks.sql.exc import DatabaseError, SessionEvictedError
from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import handle as handle_mod
from dbt.adapters.databricks.handle import CursorWrapper, DatabricksHandle, SqlUtils


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

    @pytest.mark.parametrize(
        "yaml_body, expected",
        [
            pytest.param(
                "version: 0.1\nsource: `db`.`schema`.`name`\n",
                'version: 0.1\nsource: "`db`.`schema`.`name`"\n',
                id="three_part_bare",
            ),
            pytest.param("source: `name`\n", 'source: "`name`"\n', id="one_part_bare"),
            pytest.param(
                "source: `schema`.`name`\n",
                'source: "`schema`.`name`"\n',
                id="two_part_bare",
            ),
            pytest.param(
                'source: "`db`.`schema`.`name`"\n',
                'source: "`db`.`schema`.`name`"\n',
                id="already_quoted_idempotent",
            ),
            pytest.param(
                "source: db.schema.name\n",
                "source: db.schema.name\n",
                id="no_backticks",
            ),
            pytest.param(
                (
                    "version: 0.1\n"
                    "source: `prod_db`.`analytics`.`orders`\n"
                    "joins:\n"
                    "  - name: customers\n"
                    "    source: prod_db.analytics.customers\n"
                    "    on: orders.customer_id = customers.id\n"
                    "  - name: products\n"
                    "    source: `prod_db`.`analytics`.`products` # primary product table\n"
                    "    on: orders.product_id = products.id\n"
                    "filter: orders.status = 'completed'\n"
                    "dimensions:\n"
                    "  - name: order_year\n"
                    "    expr: year(orders.created_at)\n"
                    "measures:\n"
                    "  - name: order_count\n"
                    "    expr: count(`orders`.`id`)\n"
                ),
                (
                    "version: 0.1\n"
                    'source: "`prod_db`.`analytics`.`orders`"\n'
                    "joins:\n"
                    "  - name: customers\n"
                    "    source: prod_db.analytics.customers\n"
                    "    on: orders.customer_id = customers.id\n"
                    "  - name: products\n"
                    '    source: "`prod_db`.`analytics`.`products`" # primary product table\n'
                    "    on: orders.product_id = products.id\n"
                    "filter: orders.status = 'completed'\n"
                    "dimensions:\n"
                    "  - name: order_year\n"
                    "    expr: year(orders.created_at)\n"
                    "measures:\n"
                    "  - name: order_count\n"
                    "    expr: count(`orders`.`id`)\n"
                ),
                id="realistic_mixed_metric_view",
            ),
            pytest.param(
                "expr: count(`some col`)\n",
                "expr: count(`some col`)\n",
                id="inline_backtick_in_expr",
            ),
            pytest.param(
                "  source: `db`.`s`.`n`\n",
                '  source: "`db`.`s`.`n`"\n',
                id="indented_mapping",
            ),
            pytest.param(
                "source: `schema`.`name` # a comment\n",
                'source: "`schema`.`name`" # a comment\n',
                id="trailing_comment",
            ),
            pytest.param(
                "source : `db`.`s`.`n`\n",
                'source : "`db`.`s`.`n`"\n',
                id="space_before_colon",
            ),
            pytest.param(
                "source  :   `db`.`s`.`n`\n",
                'source  :   "`db`.`s`.`n`"\n',
                id="multiple_spaces_around_colon",
            ),
            pytest.param(
                "joins:\n  - name: foo\n    source: `db`.`s`.`n`\n    on: a = b\n",
                'joins:\n  - name: foo\n    source: "`db`.`s`.`n`"\n    on: a = b\n',
                id="joins_nested_source",
            ),
            pytest.param(
                "name: `foo`\nfilter: `db`.`s`.`b`\n",
                "name: `foo`\nfilter: `db`.`s`.`b`\n",
                id="non_source_keys_untouched",
            ),
            pytest.param(
                "data_source: `db`.`s`.`n`\n",
                "data_source: `db`.`s`.`n`\n",
                id="key_with_source_substring_untouched",
            ),
            pytest.param(
                "source: `db`.`s`.`a` # primary\nsource: `db`.`s`.`b`\n",
                'source: "`db`.`s`.`a`" # primary\nsource: "`db`.`s`.`b`"\n',
                id="multi_line_mixed_comment",
            ),
            pytest.param("", "", id="empty"),
            pytest.param("   \n", "   \n", id="whitespace_only"),
        ],
    )
    def test_yaml_quote_backtick_values(self, yaml_body, expected):
        assert SqlUtils.yaml_quote_backtick_values(yaml_body) == expected

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
        assert wrapper.get_response() == AdapterResponse("OK", query_id="N/A")

    def test_get_response__with_query_id(self, cursor):
        cursor.query_id = "id"
        wrapper = CursorWrapper(cursor)
        assert wrapper.get_response() == AdapterResponse("OK", query_id="id")

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

    @pytest.fixture
    def conn_args(self):
        return {"server_hostname": "h", "http_path": "/p"}

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

    def test_handle_stashes_conn_args(self, conn):
        conn_args = {"server_hostname": "h", "http_path": "/p", "_socket_timeout": 600}
        handle = DatabricksHandle(conn, is_cluster=True, conn_args=conn_args)
        assert handle._conn_args == conn_args

    def test_handle_default_conn_args_is_none(self, conn):
        handle = DatabricksHandle(conn, is_cluster=True)
        assert handle._conn_args is None

    def test_reopen_replaces_underlying_conn(self, monkeypatch, conn_args):
        """_reopen() must swap self._conn for a fresh dbsql.connect() result."""
        original_conn = Mock()
        new_conn = Mock()
        monkeypatch.setattr(handle_mod.dbsql, "connect", Mock(return_value=new_conn))
        handle = DatabricksHandle(original_conn, is_cluster=True, conn_args=conn_args)
        handle._reopen()
        assert handle._conn is new_conn
        assert handle._conn is not original_conn
        assert handle.open is True

    def test_reopen_swallows_close_errors(self, monkeypatch, conn_args):
        """_reopen() must NOT raise when self._conn.close() raises — the server
        has already evicted the session, so the close attempt is expected to fail."""
        bad_close_conn = Mock()
        bad_close_conn.close.side_effect = Exception("Invalid SessionHandle: SessionHandle [abc]")
        new_conn = Mock()
        monkeypatch.setattr(handle_mod.dbsql, "connect", Mock(return_value=new_conn))
        handle = DatabricksHandle(bad_close_conn, is_cluster=True, conn_args=conn_args)
        # Must not raise
        handle._reopen()
        bad_close_conn.close.assert_called_once()
        assert handle._conn is new_conn

    def test_reopen_without_conn_args_raises(self):
        """A handle constructed without conn_args cannot be reopened —
        the data needed to reconnect was never captured."""
        handle = DatabricksHandle(Mock(), is_cluster=True)
        with pytest.raises(DbtRuntimeError, match="_reopen called without captured conn_args"):
            handle._reopen()

    def test_safe_execute_does_not_retry_on_non_session_evicted_error(
        self, conn, cursor, monkeypatch, conn_args
    ):
        """A plain DatabaseError (e.g. syntax error) must propagate; no reopen attempt."""
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = DatabaseError("Syntax error in SQL")

        connect_mock = Mock()
        monkeypatch.setattr(handle_mod.dbsql, "connect", connect_mock)

        handle = DatabricksHandle(conn, is_cluster=True, conn_args=conn_args)
        with pytest.raises(DatabaseError, match="Syntax error"):
            handle._safe_execute(lambda c: c.execute("BAD SQL"))
        # _reopen must NOT have been called
        connect_mock.assert_not_called()
        assert handle._conn is conn

    def test_safe_execute_retries_once_on_session_evicted_error(self, monkeypatch, conn_args):
        """First execute raises SessionEvictedError; reopen + retry must succeed."""
        first_conn = Mock()
        first_cursor = Mock()
        first_cursor.execute.side_effect = SessionEvictedError(
            "Invalid SessionHandle: SessionHandle [abc]"
        )
        first_conn.cursor.return_value = first_cursor

        new_conn = Mock()
        new_conn.cursor.return_value = Mock()

        connect_mock = Mock(return_value=new_conn)
        monkeypatch.setattr(handle_mod.dbsql, "connect", connect_mock)

        handle = DatabricksHandle(first_conn, is_cluster=True, conn_args=conn_args)
        handle._safe_execute(lambda c: c.execute("SELECT 1"))  # must not raise

        connect_mock.assert_called_once()
        assert handle._conn is new_conn

    def test_safe_execute_does_not_retry_twice(self, monkeypatch, conn_args):
        """If even the retry attempt raises SessionEvictedError, propagate."""

        def make_evicting_conn():
            c = Mock()
            cur = Mock()
            cur.execute.side_effect = SessionEvictedError("evicted again")
            c.cursor.return_value = cur
            return c

        first_conn = make_evicting_conn()
        retry_conn = make_evicting_conn()
        connect_mock = Mock(return_value=retry_conn)
        monkeypatch.setattr(handle_mod.dbsql, "connect", connect_mock)

        handle = DatabricksHandle(first_conn, is_cluster=True, conn_args=conn_args)
        with pytest.raises(SessionEvictedError, match="evicted again"):
            handle._safe_execute(lambda c: c.execute("SELECT 1"))

        connect_mock.assert_called_once()

    def test_safe_execute_without_conn_args_does_not_retry(self, conn, cursor):
        """A handle without captured conn_args (e.g. unit tests with mocks)
        must propagate SessionEvictedError immediately — no recovery."""
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = SessionEvictedError("evicted")
        handle = DatabricksHandle(conn, is_cluster=True)  # no conn_args
        with pytest.raises(SessionEvictedError):
            handle._safe_execute(lambda c: c.execute("SELECT 1"))
