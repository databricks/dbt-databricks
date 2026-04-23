from multiprocessing import get_context
from unittest.mock import Mock, patch

import pytest

from dbt.adapters.databricks.connections import (
    DatabricksConnectionManager,
    DatabricksDBTConnection,
)
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.dbr_capabilities import DBRCapabilities, DBRCapability
from dbt.adapters.databricks.utils import is_cluster_http_path


class TestDatabricksConnectionManager:
    def test_is_cluster_with_warehouse_path_no_cluster_id(self):
        """Test is_cluster() returns False for warehouse path with no cluster_id"""
        # Create a minimal connection manager with mock config
        mock_config = Mock()
        connection_manager = DatabricksConnectionManager(mock_config, get_context("spawn"))

        # Mock the connection
        mock_connection = Mock(spec=DatabricksDBTConnection)
        mock_connection.credentials = Mock(spec=DatabricksCredentials)
        mock_connection.credentials.cluster_id = None
        mock_connection.http_path = "sql/1.0/warehouses/abc123def456"

        with patch.object(
            connection_manager, "get_thread_connection", return_value=mock_connection
        ):
            assert connection_manager.is_cluster() is False

    def test_is_cluster_with_cluster_id_overrides_path(self):
        """Test is_cluster() returns False even when cluster_id is provided"""
        # Create a minimal connection manager with mock config
        mock_config = Mock()
        connection_manager = DatabricksConnectionManager(mock_config, get_context("spawn"))

        # Mock the connection with cluster_id set (overriding warehouse path)
        mock_connection = Mock(spec=DatabricksDBTConnection)
        mock_connection.credentials = Mock(spec=DatabricksCredentials)
        mock_connection.credentials.cluster_id = "cluster-123"
        mock_connection.http_path = "sql/1.0/warehouses/abc123def456"

        with patch.object(
            connection_manager, "get_thread_connection", return_value=mock_connection
        ):
            assert connection_manager.is_cluster() is False

    def test_is_cluster_http_path_function_warehouse_path(self):
        assert is_cluster_http_path("sql/1.0/warehouses/abc123def456", None) is False

    def test_is_cluster_http_path_function_cluster_path(self):
        assert is_cluster_http_path("sql/protocolv1/o/1234567890123456/", None) is True

    def test_is_cluster_http_path_function_cluster_id_overrides(self):
        assert is_cluster_http_path("sql/1.0/warehouses/abc123def456", "cluster-123") is False

    @patch("dbt.adapters.databricks.connections.DatabricksHandle.from_connection_args")
    @patch("dbt.adapters.databricks.connections.SqlUtils.prepare_connection_arguments")
    def test_open_calls_is_cluster_http_path_for_warehouse(
        self, mock_prepare_args, mock_from_connection_args
    ):
        """
        Test that open() method calls is_cluster_http_path with correct arguments for warehouse
        """
        # Create a minimal connection manager with mock config
        mock_config = Mock()
        connection_manager = DatabricksConnectionManager(mock_config, get_context("spawn"))

        # Mock the connection with proper timeout values
        mock_connection = Mock(spec=DatabricksDBTConnection)
        mock_connection.credentials = Mock(spec=DatabricksCredentials)
        mock_connection.credentials.cluster_id = None
        mock_connection.credentials.connect_retries = 1
        mock_connection.credentials.connect_timeout = 10
        mock_connection.credentials.query_tags = None
        mock_connection.credentials.is_session_mode = False  # Not session mode
        mock_connection.http_path = "sql/protocolv1/o/abc123def456"
        mock_connection.credentials.authenticate.return_value = Mock()
        mock_connection._query_header_context = None

        # Mock the handle creation
        mock_handle = Mock()
        mock_handle.session_id = "test_session"
        mock_from_connection_args.return_value = mock_handle

        mock_prepare_args.return_value = {}

        # Call open method
        connection_manager.open(mock_connection)

        # Verify that from_connection_args was called with is_cluster=False (warehouse path)
        mock_from_connection_args.assert_called_once()
        args, kwargs = mock_from_connection_args.call_args
        # Second argument (is_cluster) should be True for warehouse path with cluster_id
        assert args[1] is True


class TestTryCacheDbr:
    """Unit tests for _try_cache_dbr_capabilities."""

    HTTP_PATH = "sql/protocolv1/o/1234567890123456/cluster-abc"

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        DatabricksConnectionManager._dbr_capabilities_cache = {}
        yield
        DatabricksConnectionManager._dbr_capabilities_cache = {}

    @patch.object(DatabricksConnectionManager, "_query_dbr_version", return_value=None)
    def test_does_not_write_to_cache_when_version_is_none(self, mock_query):
        """When the version query returns None, the cache must not be written.

        This prevents a poisoned None entry from blocking the authoritative write in open().
        """
        creds = Mock(spec=DatabricksCredentials)
        creds.cluster_id = None

        DatabricksConnectionManager._try_cache_dbr_capabilities(creds, self.HTTP_PATH)

        assert self.HTTP_PATH not in DatabricksConnectionManager._dbr_capabilities_cache
        mock_query.assert_called_once_with(creds, self.HTTP_PATH)

    @patch.object(DatabricksConnectionManager, "_query_dbr_version", return_value=(15, 4))
    def test_writes_to_cache_when_version_is_known(self, mock_query):
        """When the version query succeeds, capabilities are cached correctly."""
        creds = Mock(spec=DatabricksCredentials)
        creds.cluster_id = None

        DatabricksConnectionManager._try_cache_dbr_capabilities(creds, self.HTTP_PATH)

        mock_query.assert_called_once_with(creds, self.HTTP_PATH)
        caps = DatabricksConnectionManager._dbr_capabilities_cache.get(self.HTTP_PATH)
        assert caps is not None
        assert caps.dbr_version == (15, 4)
        assert not caps.is_sql_warehouse
        assert caps.has_capability(DBRCapability.ICEBERG)

    @patch.object(DatabricksConnectionManager, "_query_dbr_version", return_value=(15, 4))
    def test_skips_write_when_already_cached(self, mock_query):
        """If the path is already in cache, the version query is never made."""
        creds = Mock(spec=DatabricksCredentials)
        creds.cluster_id = None
        existing = DBRCapabilities(dbr_version=(14, 3), is_sql_warehouse=False)
        DatabricksConnectionManager._dbr_capabilities_cache[self.HTTP_PATH] = existing

        DatabricksConnectionManager._try_cache_dbr_capabilities(creds, self.HTTP_PATH)

        mock_query.assert_not_called()
        assert DatabricksConnectionManager._dbr_capabilities_cache[self.HTTP_PATH] is existing
