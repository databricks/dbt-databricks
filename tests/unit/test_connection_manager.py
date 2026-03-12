from multiprocessing import get_context
from unittest.mock import Mock, patch

import pytest

from dbt.adapters.databricks.connections import (
    DatabricksConnectionManager,
    DatabricksDBTConnection,
    QueryContextWrapper,
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


class TestEagerCapabilityCache:
    """Tests for eager capability caching in connection creation.

    Validates the fix for the timing bug where capabilities were checked before the lazy
    connection opened, causing table_format='iceberg' to fail on named compute.
    """

    NAMED_COMPUTE_HTTP_PATH = "sql/protocolv1/o/1234567890123456/named-compute-123"
    DEFAULT_HTTP_PATH = "sql/protocolv1/o/1234567890123456/default-cluster-456"

    @pytest.fixture
    def connection_manager(self):
        mock_config = Mock()
        mock_config.credentials = Mock(spec=DatabricksCredentials)
        mock_config.credentials.http_path = self.DEFAULT_HTTP_PATH
        mock_config.credentials.compute = {
            "alternate_compute": {"http_path": self.NAMED_COMPUTE_HTTP_PATH}
        }
        mock_config.credentials.cluster_id = None
        mock_config.query_comment = Mock()
        mock_config.query_comment.comment = ""
        mock_config.query_comment.append = False

        mgr = DatabricksConnectionManager(mock_config, get_context("spawn"))
        DatabricksConnectionManager._dbr_capabilities_cache = {}
        return mgr

    @patch.object(DatabricksConnectionManager, "_cache_dbr_capabilities")
    def test_fresh_connection_caches_capabilities_before_setting_them(
        self, mock_cache_dbr, connection_manager
    ):
        """_create_fresh_connection must call _cache_dbr_capabilities before reading the cache."""
        expected_caps = DBRCapabilities(dbr_version=(15, 4), is_sql_warehouse=False)

        def populate_cache(creds, http_path):
            DatabricksConnectionManager._dbr_capabilities_cache[http_path] = expected_caps

        mock_cache_dbr.side_effect = populate_cache

        ctx = QueryContextWrapper(compute_name="alternate_compute")
        conn = connection_manager._create_fresh_connection("test_model", ctx)

        mock_cache_dbr.assert_called_once_with(
            connection_manager.profile.credentials, self.NAMED_COMPUTE_HTTP_PATH
        )
        assert conn.capabilities == expected_caps
        assert conn.capabilities.has_capability(DBRCapability.ICEBERG)

    @patch.object(DatabricksConnectionManager, "_cache_dbr_capabilities")
    def test_fresh_connection_without_eager_cache_gets_empty_defaults(
        self, mock_cache_dbr, connection_manager
    ):
        """Without the eager cache call, named compute gets empty (broken) capabilities.

        This test documents the pre-fix behavior: if _cache_dbr_capabilities is a no-op,
        the connection falls back to DBRCapabilities() which has all capabilities disabled.
        """
        ctx = QueryContextWrapper(compute_name="alternate_compute")
        conn = connection_manager._create_fresh_connection("test_model", ctx)

        assert conn.capabilities.dbr_version is None
        assert not conn.capabilities.is_sql_warehouse
        assert not conn.capabilities.has_capability(DBRCapability.ICEBERG)

    @patch.object(DatabricksConnectionManager, "_cache_dbr_capabilities")
    def test_default_compute_cache_is_idempotent(self, mock_cache_dbr, connection_manager):
        """For default compute with a warm cache, the eager call is a no-op."""
        warm_caps = DBRCapabilities(dbr_version=(15, 4), is_sql_warehouse=False)
        DatabricksConnectionManager._dbr_capabilities_cache[self.DEFAULT_HTTP_PATH] = warm_caps

        ctx = QueryContextWrapper()
        conn = connection_manager._create_fresh_connection("test_model", ctx)

        mock_cache_dbr.assert_called_once()
        assert conn.capabilities == warm_caps
