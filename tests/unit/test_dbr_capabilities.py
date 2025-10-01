"""
Unit tests for the DBR capability system.
"""
import pytest
from dbt.adapters.databricks.dbr_capabilities import (
    DBRCapability,
    DBRCapabilities,
    CapabilitySpec,
)


class TestDBRCapabilities:
    """Test the DBR capability system."""

    def test_capability_enum_values(self):
        """Test that all capabilities have the expected values."""
        assert DBRCapability.TIMESTAMPDIFF.value == "timestampdiff"
        assert DBRCapability.ICEBERG.value == "iceberg"
        assert DBRCapability.COMMENT_ON_COLUMN.value == "comment_on_column"
        assert DBRCapability.JSON_COLUMN_METADATA.value == "json_column_metadata"

    def test_old_dbr_version(self):
        """Test capabilities with old DBR version."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        # Should not have newer features
        assert not capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_id, dbr_version=(10, 0)
        )
        assert not capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=(10, 0)
        )
        assert not capabilities.has_capability(
            DBRCapability.COMMENT_ON_COLUMN, compute_id, dbr_version=(10, 0)
        )
        assert not capabilities.has_capability(
            DBRCapability.JSON_COLUMN_METADATA, compute_id, dbr_version=(10, 0)
        )

    def test_modern_dbr_version(self):
        """Test capabilities with modern DBR version."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        # Should have all features up to 16.2
        assert capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_id, dbr_version=(16, 2)
        )
        assert capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=(16, 2)
        )
        assert capabilities.has_capability(
            DBRCapability.COMMENT_ON_COLUMN, compute_id, dbr_version=(16, 2)
        )
        assert capabilities.has_capability(
            DBRCapability.JSON_COLUMN_METADATA, compute_id, dbr_version=(16, 2)
        )

    def test_sql_warehouse(self):
        """Test that SQL warehouses are assumed to have latest features."""
        capabilities = DBRCapabilities()
        compute_id = "sql-warehouse"

        # SQL warehouses should have all supported features
        assert capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_id, is_sql_warehouse=True
        )
        assert capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, is_sql_warehouse=True
        )
        assert capabilities.has_capability(
            DBRCapability.COMMENT_ON_COLUMN, compute_id, is_sql_warehouse=True
        )
        assert capabilities.has_capability(
            DBRCapability.JSON_COLUMN_METADATA, compute_id, is_sql_warehouse=True
        )

    def test_sql_warehouse_unsupported_features(self):
        """Test that some features are not supported on SQL warehouses."""
        capabilities = DBRCapabilities()
        compute_id = "sql-warehouse"

        # Streaming table features not supported on SQL warehouses yet
        assert not capabilities.has_capability(
            DBRCapability.STREAMING_TABLE_JSON_METADATA, compute_id, is_sql_warehouse=True
        )

    def test_unity_catalog_requirements(self):
        """Test Unity Catalog specific requirements."""
        capabilities = DBRCapabilities()

        # Without Unity Catalog, Unity-specific features should be disabled
        # (Currently no capabilities require Unity Catalog, but the infrastructure is there)
        compute_no_uc = "compute-no-uc"
        compute_with_uc = "compute-with-uc"

        # Test with and without Unity Catalog (future-proofing the test)
        # Both should work for current capabilities since none require UC
        assert capabilities.has_capability(
            DBRCapability.ICEBERG, compute_no_uc, dbr_version=(16, 2), is_unity_catalog=False
        )
        assert capabilities.has_capability(
            DBRCapability.ICEBERG, compute_with_uc, dbr_version=(16, 2), is_unity_catalog=True
        )

    def test_capability_overrides(self):
        """Test manual capability overrides."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        overrides = {
            DBRCapability.ICEBERG: False,  # Manually disable
            DBRCapability.TIMESTAMPDIFF: True,  # Manually enable
        }

        # Override should disable Iceberg even though version supports it
        assert not capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=(16, 2), capability_overrides=overrides
        )

        # Should still have other capabilities
        assert capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_id, dbr_version=(16, 2), capability_overrides=overrides
        )
        assert capabilities.has_capability(
            DBRCapability.COMMENT_ON_COLUMN, compute_id, dbr_version=(16, 2)
        )

    def test_get_required_version(self):
        """Test getting required version strings."""
        capabilities = DBRCapabilities()

        assert capabilities.get_required_version(DBRCapability.TIMESTAMPDIFF) == "DBR 10.4+"
        assert capabilities.get_required_version(DBRCapability.ICEBERG) == "DBR 14.3+"
        assert capabilities.get_required_version(DBRCapability.COMMENT_ON_COLUMN) == "DBR 16.1+"

    def test_capability_caching(self):
        """Test that capability checks are cached per compute."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        # First check should populate cache
        result1 = capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=(16, 2)
        )

        # Second check should use cache (no need to pass version again)
        result2 = capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=(16, 2)
        )

        assert result1 == result2 == True

        # Cache should contain the result
        assert compute_id in capabilities._capability_cache_by_compute
        assert DBRCapability.ICEBERG in capabilities._capability_cache_by_compute[compute_id]

    def test_set_capability_clears_cache(self):
        """Test that setting a capability clears its cache."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        # Populate cache
        capabilities.has_capability(DBRCapability.ICEBERG, compute_id, dbr_version=(16, 2))
        assert compute_id in capabilities._capability_cache_by_compute
        assert DBRCapability.ICEBERG in capabilities._capability_cache_by_compute[compute_id]

        # Set capability should clear cache for that capability
        capabilities.set_capability(DBRCapability.ICEBERG, False, compute_id)

        # Cache for that capability should be cleared
        assert DBRCapability.ICEBERG not in capabilities._capability_cache_by_compute.get(compute_id, {})

    def test_no_connection(self):
        """Test behavior when not connected."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        # Without connection info (dbr_version=None), assume no capabilities
        assert not capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_id, dbr_version=None
        )
        assert not capabilities.has_capability(
            DBRCapability.ICEBERG, compute_id, dbr_version=None
        )

    def test_enabled_capabilities_property(self):
        """Test the enabled_capabilities method."""
        capabilities = DBRCapabilities()
        compute_id = "test-compute"

        enabled = capabilities.enabled_capabilities(
            compute_id, dbr_version=(16, 2)
        )

        # Should include all capabilities supported by DBR 16.2
        expected = {
            DBRCapability.TIMESTAMPDIFF,
            DBRCapability.ICEBERG,
            DBRCapability.COMMENT_ON_COLUMN,
            DBRCapability.JSON_COLUMN_METADATA,
        }

        assert expected.issubset(enabled)

        # Should not include capabilities requiring newer versions
        assert DBRCapability.STREAMING_TABLE_JSON_METADATA not in enabled


class TestPerComputeCaching:
    """Test the per-compute caching functionality."""

    def test_multiple_compute_resources(self):
        """Test that different compute resources have separate capability caches."""
        capabilities = DBRCapabilities()

        # Test compute 1: Old DBR cluster
        compute_1 = "sql/protocolv1/o/123/cluster-1"
        assert not capabilities.has_capability(
            DBRCapability.ICEBERG,
            compute_1,
            dbr_version=(10, 0),
            is_sql_warehouse=False,
        )

        # Test compute 2: New SQL warehouse
        compute_2 = "sql/1.0/warehouses/warehouse-1"
        assert capabilities.has_capability(
            DBRCapability.ICEBERG,
            compute_2,
            dbr_version=None,  # SQL warehouses don't need version
            is_sql_warehouse=True,
        )

        # Verify compute 1 still returns same result (cached separately)
        assert not capabilities.has_capability(
            DBRCapability.ICEBERG,
            compute_1,
            dbr_version=(10, 0),
            is_sql_warehouse=False,
        )

        # Verify caching - should have two separate caches
        assert len(capabilities._capability_cache_by_compute) == 2
        assert compute_1 in capabilities._capability_cache_by_compute
        assert compute_2 in capabilities._capability_cache_by_compute

    def test_clear_compute_cache(self):
        """Test clearing cache for specific compute."""
        capabilities = DBRCapabilities()

        # Add capabilities to two computes
        compute_1 = "compute-1"
        compute_2 = "compute-2"

        capabilities.has_capability(
            DBRCapability.ICEBERG, compute_1, dbr_version=(14, 3)
        )
        capabilities.has_capability(
            DBRCapability.TIMESTAMPDIFF, compute_2, dbr_version=(10, 4)
        )

        assert len(capabilities._capability_cache_by_compute) == 2

        # Clear cache for compute_1
        capabilities.clear_compute_cache(compute_1)

        # Verify compute_1 cache is cleared but compute_2 remains
        assert compute_1 not in capabilities._capability_cache_by_compute
        assert compute_2 in capabilities._capability_cache_by_compute
        assert len(capabilities._capability_cache_by_compute) == 1

    def test_compute_metadata_storage(self):
        """Test that compute metadata is stored correctly."""
        capabilities = DBRCapabilities()

        compute_id = "test-compute"
        capabilities.has_capability(
            DBRCapability.ICEBERG,
            compute_id,
            dbr_version=(14, 3),
            is_sql_warehouse=False,
            is_unity_catalog=True,
        )

        # Check stored metadata
        assert compute_id in capabilities._compute_metadata
        metadata = capabilities._compute_metadata[compute_id]
        assert metadata["dbr_version"] == (14, 3)
        assert metadata["is_sql_warehouse"] is False
        assert metadata["is_unity_catalog"] is True


class TestCapabilitySpec:
    """Test the CapabilitySpec dataclass."""

    def test_capability_spec_creation(self):
        """Test creating a capability spec."""
        spec = CapabilitySpec(
            capability=DBRCapability.ICEBERG,
            min_version=(14, 3),
            requires_unity_catalog=False,
            sql_warehouse_supported=True,
        )

        assert spec.capability == DBRCapability.ICEBERG
        assert spec.min_version == (14, 3)
        assert spec.requires_unity_catalog is False
        assert spec.sql_warehouse_supported is True

    def test_capability_spec_defaults(self):
        """Test capability spec defaults."""
        spec = CapabilitySpec(
            capability=DBRCapability.TIMESTAMPDIFF,
            min_version=(10, 4),
        )

        # Should use defaults
        assert spec.requires_unity_catalog is False
        assert spec.sql_warehouse_supported is True
        assert spec.enabled_by_default is True