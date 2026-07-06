import base64
import json
import os
import sys
from decimal import Decimal
from typing import Any
from unittest.mock import Mock, patch

import pytest
from databricks.sql.client import Cursor
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError

from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)
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


_KERNEL_HTTP_PATH = "/sql/1.0/warehouses/abc"


def _prepare_connection_args(**creds_kwargs: Any) -> dict[str, Any]:
    """Run prepare_connection_arguments for creds built from fixed host/path/schema
    plus the given auth kwargs, and return the resulting connect() kwargs."""
    creds = DatabricksCredentials(
        host="yourorg.databricks.com",
        http_path=_KERNEL_HTTP_PATH,
        schema="dbt",
        **creds_kwargs,
    )
    manager = DatabricksCredentialManager.create_from(creds)
    return SqlUtils.prepare_connection_arguments(creds, manager, _KERNEL_HTTP_PATH, {})


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

    def test_prepare_connection_arguments__default_uses_credentials_provider(self):
        """Without use_kernel, the connector's credentials_provider is passed and
        no raw credentials leak into the connect() kwargs."""
        args = _prepare_connection_args(client_id="cid", client_secret="dose-secret")
        assert callable(args["credentials_provider"])
        assert "use_kernel" not in args
        assert "oauth_client_id" not in args
        assert "access_token" not in args

    def _kernel_args_with_token(self, bearer, **creds_kwargs):
        """Build kernel connect() kwargs with the credential manager's
        header_factory stubbed to yield the given Authorization header, so the
        machine-auth path can be exercised without live authentication."""
        creds = DatabricksCredentials(
            host="yourorg.databricks.com",
            http_path=_KERNEL_HTTP_PATH,
            schema="dbt",
            connection_parameters={"use_kernel": True},
            **creds_kwargs,
        )
        manager = DatabricksCredentialManager.create_from(creds)
        header = {"Authorization": bearer} if bearer is not None else {}
        with patch.object(
            type(manager),
            "header_factory",
            new_callable=lambda: property(lambda self: (lambda: header)),
        ):
            return SqlUtils.prepare_connection_arguments(creds, manager, _KERNEL_HTTP_PATH, {})

    def test_prepare_connection_arguments__kernel_pat_forwards_resolved_token(self):
        """use_kernel with a PAT resolves the bearer token from the credential
        manager and forwards it as access_token, dropping credentials_provider."""
        args = self._kernel_args_with_token("Bearer dapiabc123", token="dapiabc123")
        assert args["use_kernel"] is True
        assert args["access_token"] == "dapiabc123"
        assert args.get("credentials_provider") is None
        assert "oauth_client_id" not in args

    def test_prepare_connection_arguments__kernel_m2m_forwards_resolved_token(self):
        """OAuth M2M is resolved to a Databricks workspace token by the credential
        manager, and that token is forwarded to the kernel's PAT path — not the raw
        client id/secret."""
        args = self._kernel_args_with_token(
            "Bearer exchanged-m2m-token", client_id="cid", client_secret="dose-secret"
        )
        assert args["access_token"] == "exchanged-m2m-token"
        assert args.get("credentials_provider") is None
        assert "oauth_client_secret" not in args

    def test_prepare_connection_arguments__kernel_azure_sp_forwards_resolved_token(self):
        """The Azure-AD service principal (the peco CI credential) is resolved to a
        Databricks workspace token by the credential manager's azure-client-secret
        auth, and that token drives the kernel's PAT path. This is what makes the
        kernel work for Azure SP — forwarding the raw creds to the kernel's
        Databricks-OAuth M2M would fail with invalid_client."""
        args = self._kernel_args_with_token(
            "Bearer azure-exchanged-token",
            azure_client_id="acid",
            azure_client_secret="asecret",
        )
        assert args["access_token"] == "azure-exchanged-token"
        assert args.get("credentials_provider") is None
        assert "oauth_client_id" not in args

    def test_prepare_connection_arguments__kernel_non_bearer_header_raises(self):
        """If the credential manager yields a non-Bearer (or missing) Authorization
        header, there is no token to forward — fail loudly rather than hand the
        kernel unusable auth."""
        with pytest.raises(DbtConfigError):
            self._kernel_args_with_token("Basic abc123", token="dapiabc123")

    def test_prepare_connection_arguments__kernel_u2m_explicit_client_id(self):
        """use_kernel with OAuth U2M and an explicit client_id forwards
        oauth_client_id and translates dbt's auth_type='oauth' to the kernel's
        'databricks-oauth' so the kernel bridge routes to its U2M flow. U2M is a
        browser flow the kernel runs itself, so no token is pre-resolved."""
        args = _prepare_connection_args(
            client_id="cid",
            auth_type="oauth",
            connection_parameters={"use_kernel": True},
        )
        assert args["use_kernel"] is True
        assert args["oauth_client_id"] == "cid"
        assert args["auth_type"] == "databricks-oauth"
        assert args.get("credentials_provider") is None
        assert "oauth_client_secret" not in args
        assert "access_token" not in args

    def test_prepare_connection_arguments__kernel_u2m_default_app_no_client_id(self):
        """The common external-browser config is `auth_type: oauth` with NO
        client_id (SDK uses the default dbt app). The kernel bridge routes U2M off
        auth_type alone, so we must translate auth_type even without a client_id."""
        args = _prepare_connection_args(
            auth_type="oauth",
            connection_parameters={"use_kernel": True},
        )
        assert args["auth_type"] == "databricks-oauth"
        assert args.get("credentials_provider") is None
        assert "oauth_client_id" not in args
        assert "oauth_client_secret" not in args
        assert "access_token" not in args


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

    @pytest.mark.parametrize("raw", ["null", "[]", "123", '"foo"', "true"])
    def test_outer_json_non_object(self, raw):
        """Outer JSON is valid but not an object — must not raise."""
        with patch.dict(os.environ, {"DBT_DATABRICKS_HTTP_SESSION_HEADERS": raw}):
            ctx = _get_job_run_context()
        assert ctx == {"job_id": None, "job_run_id": None, "task_run_id": None}

    @pytest.mark.parametrize("inner_payload", [b"[1,2,3]", b"null", b"123", b'"x"', b"true"])
    def test_inner_base64_json_non_object(self, inner_payload):
        """Inner base64 decodes to valid but non-object JSON — job_run_id stays None,
        outer fields still resolve."""
        bad_source = base64.b64encode(inner_payload).decode()
        with patch.dict(
            os.environ,
            {
                "DBT_DATABRICKS_HTTP_SESSION_HEADERS": _build_session_headers(
                    source_override=bad_source
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
