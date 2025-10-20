"""
Integration tests for adapter capability system.
"""

from multiprocessing import get_context
from unittest.mock import Mock, patch

import pytest
from dbt_common.exceptions import DbtConfigError

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
        assert hasattr(adapter, "has_dbr_capability")
        assert hasattr(adapter, "require_capability")

    def test_capability_initialization_lazy(self, adapter):
        """Test that capabilities are now owned by connections."""
        # Capabilities are now owned by each connection, not the adapter
        # The adapter delegates to the connection
        assert not hasattr(adapter, "_dbr_capabilities")

    def test_has_dbr_capability_string_interface(self, adapter):
        """Test that has_dbr_capability accepts string capability names."""
        # Mock a connection with DBR 16.2
        mock_conn = Mock()
        mock_conn._capabilities = DBRCapabilities(dbr_version=(16, 2), is_sql_warehouse=False)
        mock_conn.has_capability = mock_conn._capabilities.has_capability

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Test with valid capability names
            assert adapter.has_dbr_capability("timestampdiff")
            assert adapter.has_dbr_capability("comment_on_column")
            assert adapter.has_dbr_capability("json_column_metadata")

            # Test with invalid capability name - should raise ValueError
            with pytest.raises(ValueError, match="Unknown DBR capability"):
                adapter.has_dbr_capability("nonexistent")

    def test_require_capability_success(self, adapter):
        """Test require_capability with supported capability."""
        # Mock a connection that has the capability
        mock_conn = Mock()
        mock_conn.has_capability = Mock(return_value=True)

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Should not raise
            adapter.require_capability(DBRCapability.ICEBERG)

    def test_require_capability_failure(self, adapter):
        """Test require_capability with unsupported capability."""
        # Mock a connection that doesn't have the capability
        mock_conn = Mock()
        mock_conn.has_capability = Mock(return_value=False)

        with patch.object(adapter.connections, "get_thread_connection", return_value=mock_conn):
            # Should raise DbtConfigError
            with pytest.raises(DbtConfigError, match="iceberg.*requires.*DBR 14.3"):
                adapter.require_capability(DBRCapability.ICEBERG)

    def test_sql_warehouse_capabilities(self, adapter):
        """Test adapter capability checking method."""
        # Mock a connection that reports it's a SQL warehouse
        mock_conn = Mock()
        mock_conn.http_path = "sql/1.0/warehouses/test"
        mock_conn.credentials = Mock(catalog="main")

        # Create a real capabilities object for testing with SQL warehouse settings
        mock_conn._capabilities = DBRCapabilities(dbr_version=None, is_sql_warehouse=True)

        def mock_has_capability(cap):
            return mock_conn._capabilities.has_capability(cap)

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

    def test_insert_by_name_capability_spec(self):
        """Test INSERT_BY_NAME capability specification and requirements."""
        # Verify the capability exists
        assert DBRCapability.INSERT_BY_NAME in DBRCapabilities.CAPABILITY_SPECS

        # Get the capability spec
        spec = DBRCapabilities.CAPABILITY_SPECS[DBRCapability.INSERT_BY_NAME]

        # Verify the specification
        assert spec.min_version == (12, 2), "INSERT_BY_NAME should require DBR 12.2+"
        assert spec.sql_warehouse_supported is True, (
            "INSERT_BY_NAME should be supported in SQL warehouses"
        )

    def test_insert_by_name_capability_with_dbr_12_2(self):
        """Test INSERT_BY_NAME capability is available on DBR 12.2+"""
        capabilities = DBRCapabilities(dbr_version=(12, 2))
        assert capabilities.has_capability(DBRCapability.INSERT_BY_NAME)

    def test_insert_by_name_capability_with_dbr_12_1(self):
        """Test INSERT_BY_NAME capability is NOT available on DBR 12.1"""
        capabilities = DBRCapabilities(dbr_version=(12, 1))
        assert not capabilities.has_capability(DBRCapability.INSERT_BY_NAME)

    def test_insert_by_name_capability_with_sql_warehouse(self):
        """Test INSERT_BY_NAME capability is available in SQL warehouses"""
        capabilities = DBRCapabilities(is_sql_warehouse=True)
        assert capabilities.has_capability(DBRCapability.INSERT_BY_NAME)

    def test_insert_by_name_required_version_string(self):
        """Test that the required version string is correct for INSERT_BY_NAME"""
        version_string = DBRCapabilities.get_required_version(DBRCapability.INSERT_BY_NAME)
        assert version_string == "DBR 12.2+"
