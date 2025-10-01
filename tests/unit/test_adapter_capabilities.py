"""
Integration tests for adapter capability system.
"""

from multiprocessing import get_context
from unittest.mock import Mock, patch

import pytest

from dbt.adapters.databricks.dbr_capabilities import DBRCapabilities, DBRCapability
from dbt.adapters.databricks.impl import DatabricksAdapter
from tests.unit.utils import config_from_parts_or_dicts


class TestAdapterCapabilities:
    """Test capability integration with DatabricksAdapter."""

    @pytest.fixture
    def adapter(self) -> DatabricksAdapter:
        """Create a mock adapter for testing."""
        project_cfg = {
            "name": "test_project",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "databricks",
                    "catalog": "main",
                    "schema": "analytics",
                    "host": "test.databricks.com",
                    "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                    "token": "dapi" + "X" * 32,
                }
            },
            "target": "test",
        }

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        return DatabricksAdapter(config, get_context("spawn"))

    def test_adapter_has_capability_method(self, adapter):
        """Test that adapter has capability methods."""
        assert hasattr(adapter, "has_capability")
        assert hasattr(adapter, "require_capability")
        assert hasattr(adapter, "has_dbr_capability")

    def test_capability_initialization_lazy(self, adapter):
        """Test that capabilities are now owned by connections."""
        # Capabilities are now owned by each connection, not the adapter
        # The adapter delegates to the connection
        assert not hasattr(adapter, "_dbr_capabilities")

    def test_has_dbr_capability_method(self, adapter):
        """Test the has_dbr_capability method used by macros."""
        # Mock a connection with capability support
        mock_conn = Mock()
        mock_conn.has_capability = Mock(
            side_effect=lambda cap: cap.value != "invalid_capability"
            if hasattr(cap, "value")
            else False
        )

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Test valid capability
            assert adapter.has_dbr_capability("json_column_metadata")

            # Test invalid capability name (should return False for unknown capabilities)
            assert not adapter.has_dbr_capability("invalid_capability")

    def test_require_capability_success(self, adapter):
        """Test require_capability with supported capability."""
        # Mock a connection that has the capability
        mock_conn = Mock()
        mock_conn.has_capability = Mock(return_value=True)

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Should not raise
            adapter.require_capability(DBRCapability.ICEBERG, "Test feature")

    def test_require_capability_failure(self, adapter):
        """Test require_capability with unsupported capability."""
        # Mock a connection that doesn't have the capability
        mock_conn = Mock()
        mock_conn.has_capability = Mock(return_value=False)

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Should raise DbtConfigError
            from dbt_common.exceptions import DbtConfigError

            with pytest.raises(DbtConfigError, match="Test feature.*requires.*DBR 14.3"):
                adapter.require_capability(DBRCapability.ICEBERG, "Test feature")

    def test_sql_warehouse_capabilities(self, adapter):
        """Test adapter capability checking method."""
        # Mock a connection that reports it's a SQL warehouse
        mock_conn = Mock()
        mock_conn.http_path = "sql/1.0/warehouses/test"
        mock_conn.handle = Mock()
        mock_conn.handle.dbr_version = None
        mock_conn.handle.is_sql_warehouse = True
        mock_conn.credentials = Mock(catalog="main")

        # Create a real capabilities object for testing
        mock_conn._capabilities = DBRCapabilities()
        mock_conn.capabilities = mock_conn._capabilities

        def mock_has_capability(cap):
            return mock_conn._capabilities.has_capability(
                cap, mock_conn.http_path, None, True, True
            )

        mock_conn.has_capability = mock_has_capability

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Should have most capabilities
            assert adapter.has_capability(DBRCapability.TIMESTAMPDIFF)
            assert adapter.has_capability(DBRCapability.ICEBERG)
            assert adapter.has_capability(DBRCapability.COMMENT_ON_COLUMN)
            assert adapter.has_capability(DBRCapability.JSON_COLUMN_METADATA)

            # But not streaming table features
            assert not adapter.has_capability(DBRCapability.STREAMING_TABLE_JSON_METADATA)

    def test_capability_caching_in_adapter(self, adapter):
        """Test that adapter delegates to connection for capability caching."""
        # Mock a connection with capabilities
        mock_conn = Mock()
        mock_conn.has_capability = Mock(return_value=True)

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # First call
            result1 = adapter.has_capability(DBRCapability.ICEBERG)
            # Second call
            result2 = adapter.has_capability(DBRCapability.ICEBERG)

            assert result1 is True
            assert result2 is True
            # Connection should have been asked twice (adapter doesn't cache)
            assert mock_conn.has_capability.call_count == 2
