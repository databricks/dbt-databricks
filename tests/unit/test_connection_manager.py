from multiprocessing import get_context
from unittest.mock import Mock, patch

from dbt.adapters.databricks.connections import DatabricksConnectionManager, DatabricksDBTConnection
from dbt.adapters.databricks.credentials import DatabricksCredentials
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
        mock_connection.http_path = "sql/protocolv1/o/abc123def456"
        mock_connection.credentials.authenticate.return_value = Mock()

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
