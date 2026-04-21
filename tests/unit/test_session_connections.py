"""Unit tests for DatabricksConnectionManager session mode methods."""

from multiprocessing import get_context
from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.session import DatabricksSessionHandle, SessionCursorWrapper


def _make_manager(is_session: bool) -> DatabricksConnectionManager:
    """Create a DatabricksConnectionManager with mocked profile credentials."""
    mock_config = MagicMock()
    mock_config.credentials.is_session_mode = is_session
    return DatabricksConnectionManager(mock_config, get_context("spawn"))


class TestApiClientSessionMode:
    """api_client raises in session mode."""

    def test_api_client_raises_in_session_mode(self):
        manager = _make_manager(is_session=True)
        with pytest.raises(DbtRuntimeError, match="not available in session mode"):
            _ = manager.api_client


class TestIsClusterSessionMode:
    """is_cluster returns True in session mode without checking http_path."""

    def test_is_cluster_returns_true_in_session_mode(self):
        manager = _make_manager(is_session=True)
        assert manager.is_cluster() is True


class TestCancelOpenSessionMode:
    """cancel_open skips PythonRunTracker in session mode."""

    def test_cancel_open_skips_python_tracker_in_session_mode(self):
        manager = _make_manager(is_session=True)

        with (
            patch("dbt.adapters.databricks.connections.PythonRunTracker") as mock_tracker,
            patch(
                "dbt.adapters.spark.connections.SparkConnectionManager.cancel_open",
                return_value=[],
            ),
        ):
            result = manager.cancel_open()

        mock_tracker.cancel_runs.assert_not_called()
        assert result == []


class TestCacheSessionCapabilities:
    """_cache_session_capabilities caches on first call, skips on second."""

    def setup_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def teardown_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def test_caches_capabilities_on_first_call(self):
        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.dbr_version = (14, 3)

        DatabricksConnectionManager._cache_session_capabilities(mock_handle)

        caps = DatabricksConnectionManager._session_capabilities
        assert caps is not None
        assert caps.dbr_version == (14, 3)
        assert caps.is_sql_warehouse is False

    def test_skips_cache_on_second_call(self):
        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.dbr_version = (14, 3)

        DatabricksConnectionManager._cache_session_capabilities(mock_handle)
        mock_handle.dbr_version = (15, 0)
        DatabricksConnectionManager._cache_session_capabilities(mock_handle)

        assert DatabricksConnectionManager._session_capabilities.dbr_version == (14, 3)


class TestOpenSession:
    """_open_session sets handle and state on the connection."""

    def setup_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def teardown_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def test_open_session_sets_handle_and_state(self):
        from dbt.adapters.contracts.connection import ConnectionState

        mock_creds = MagicMock()
        mock_creds.database = "my_catalog"
        mock_creds.schema = "my_schema"
        mock_creds.session_properties = {}

        mock_connection = MagicMock()
        mock_connection.state = ConnectionState.INIT

        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.session_id = "app-123"
        mock_handle.dbr_version = (14, 3)

        with patch(
            "dbt.adapters.databricks.connections.DatabricksSessionHandle.create",
            return_value=mock_handle,
        ):
            result = DatabricksConnectionManager._open_session(mock_connection, mock_creds)

        assert result.state == ConnectionState.OPEN
        assert result.handle is mock_handle

    def test_open_session_wraps_exception_in_dbt_database_error(self):
        from dbt_common.exceptions import DbtDatabaseError

        mock_creds = MagicMock()
        mock_creds.database = "my_catalog"
        mock_creds.schema = "my_schema"
        mock_creds.session_properties = {}

        mock_connection = MagicMock()

        with patch(
            "dbt.adapters.databricks.connections.DatabricksSessionHandle.create",
            side_effect=RuntimeError("spark unavailable"),
        ):
            with pytest.raises(DbtDatabaseError, match="Failed to create session connection"):
                DatabricksConnectionManager._open_session(mock_connection, mock_creds)


class TestGetResponseSessionCursor:
    """get_response handles SessionCursorWrapper correctly."""

    def test_get_response_with_session_cursor(self):
        mock_spark = MagicMock()
        cursor = SessionCursorWrapper(mock_spark)

        response = DatabricksConnectionManager.get_response(cursor)

        assert response._message == "OK"
        assert response.query_id == "session-query"
