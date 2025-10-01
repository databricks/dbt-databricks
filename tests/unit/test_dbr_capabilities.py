"""
Unit tests for the DBR capability system.
"""

from dbt.adapters.databricks.dbr_capabilities import (
    CapabilitySpec,
    DBRCapabilities,
    DBRCapability,
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
        capabilities = DBRCapabilities(dbr_version=(10, 0))

        # Should not have newer features
        assert not capabilities.has_capability(DBRCapability.TIMESTAMPDIFF)
        assert not capabilities.has_capability(DBRCapability.ICEBERG)
        assert not capabilities.has_capability(DBRCapability.COMMENT_ON_COLUMN)
        assert not capabilities.has_capability(DBRCapability.JSON_COLUMN_METADATA)

    def test_modern_dbr_version(self):
        """Test capabilities with modern DBR version."""
        capabilities = DBRCapabilities(dbr_version=(16, 2))

        # Should have all features up to 16.2
        assert capabilities.has_capability(DBRCapability.TIMESTAMPDIFF)
        assert capabilities.has_capability(DBRCapability.ICEBERG)
        assert capabilities.has_capability(DBRCapability.COMMENT_ON_COLUMN)
        assert capabilities.has_capability(DBRCapability.JSON_COLUMN_METADATA)

    def test_sql_warehouse(self):
        """Test that SQL warehouses are assumed to have latest features."""
        capabilities = DBRCapabilities(is_sql_warehouse=True)

        # SQL warehouses should have all supported features
        assert capabilities.has_capability(DBRCapability.TIMESTAMPDIFF)
        assert capabilities.has_capability(DBRCapability.ICEBERG)
        assert capabilities.has_capability(DBRCapability.COMMENT_ON_COLUMN)
        assert capabilities.has_capability(DBRCapability.JSON_COLUMN_METADATA)

    def test_sql_warehouse_unsupported_features(self):
        """Test that some features are not supported on SQL warehouses."""
        capabilities = DBRCapabilities(is_sql_warehouse=True)

        # Streaming table features not supported on SQL warehouses yet
        assert not capabilities.has_capability(DBRCapability.STREAMING_TABLE_JSON_METADATA)

    def test_unity_catalog_requirements(self):
        """Test Unity Catalog specific requirements."""
        # Without Unity Catalog
        capabilities_no_uc = DBRCapabilities(dbr_version=(16, 2), is_unity_catalog=False)
        # With Unity Catalog
        capabilities_with_uc = DBRCapabilities(dbr_version=(16, 2), is_unity_catalog=True)

        # Test with and without Unity Catalog (future-proofing the test)
        # Both should work for current capabilities since none require UC
        assert capabilities_no_uc.has_capability(DBRCapability.ICEBERG)
        assert capabilities_with_uc.has_capability(DBRCapability.ICEBERG)

    def test_capability_overrides(self):
        """Test manual capability overrides."""
        overrides = {
            DBRCapability.ICEBERG: False,  # Manually disable
            DBRCapability.TIMESTAMPDIFF: True,  # Manually enable
        }

        capabilities = DBRCapabilities(dbr_version=(16, 2), capability_overrides=overrides)

        # Override should disable Iceberg even though version supports it
        assert not capabilities.has_capability(DBRCapability.ICEBERG)

        # Override enables TIMESTAMPDIFF
        assert capabilities.has_capability(DBRCapability.TIMESTAMPDIFF)

        # Should still have other capabilities
        assert capabilities.has_capability(DBRCapability.COMMENT_ON_COLUMN)

    def test_get_required_version(self):
        """Test getting required version strings."""
        assert DBRCapabilities.get_required_version(DBRCapability.TIMESTAMPDIFF) == "DBR 10.4+"
        assert DBRCapabilities.get_required_version(DBRCapability.ICEBERG) == "DBR 14.3+"
        assert DBRCapabilities.get_required_version(DBRCapability.COMMENT_ON_COLUMN) == "DBR 16.1+"

    def test_capability_caching(self):
        """Test that capability checks are cached."""
        capabilities = DBRCapabilities(dbr_version=(16, 2))

        # First check should populate cache
        result1 = capabilities.has_capability(DBRCapability.ICEBERG)

        # Second check should use cache
        result2 = capabilities.has_capability(DBRCapability.ICEBERG)

        assert result1 is True
        assert result2 is True

        # Cache should contain the result
        assert DBRCapability.ICEBERG in capabilities._capability_cache

    def test_set_capability_clears_cache(self):
        """Test that setting a capability clears its cache."""
        capabilities = DBRCapabilities(dbr_version=(16, 2))

        # Populate cache
        capabilities.has_capability(DBRCapability.ICEBERG)
        assert DBRCapability.ICEBERG in capabilities._capability_cache

        # Set capability should clear cache for that capability
        capabilities.set_capability(DBRCapability.ICEBERG, False)

        # Cache for that capability should be cleared
        assert DBRCapability.ICEBERG not in capabilities._capability_cache

        # New check should use override
        assert not capabilities.has_capability(DBRCapability.ICEBERG)

    def test_no_connection(self):
        """Test behavior when not connected (no version info)."""
        capabilities = DBRCapabilities(dbr_version=None)

        # Without connection info, assume no capabilities
        assert not capabilities.has_capability(DBRCapability.TIMESTAMPDIFF)
        assert not capabilities.has_capability(DBRCapability.ICEBERG)

    def test_enabled_capabilities_property(self):
        """Test the enabled_capabilities method."""
        capabilities = DBRCapabilities(dbr_version=(16, 2))

        enabled = capabilities.enabled_capabilities()

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


class TestPerComputeInstances:
    """Test that different compute resources have separate capability instances."""

    def test_multiple_compute_resources(self):
        """Test that different compute resources can have different capabilities."""
        # Compute 1: Old DBR cluster
        capabilities_1 = DBRCapabilities(dbr_version=(10, 0), is_sql_warehouse=False)
        assert not capabilities_1.has_capability(DBRCapability.ICEBERG)

        # Compute 2: New SQL warehouse
        capabilities_2 = DBRCapabilities(dbr_version=None, is_sql_warehouse=True)
        assert capabilities_2.has_capability(DBRCapability.ICEBERG)

        # Verify compute 1 still returns same result (separate instances)
        assert not capabilities_1.has_capability(DBRCapability.ICEBERG)

    def test_updating_capabilities_for_new_compute(self):
        """Test that recreating a capabilities object reflects new compute info."""
        # Start with old version
        old_capabilities = DBRCapabilities(dbr_version=(10, 0))
        assert not old_capabilities.has_capability(DBRCapability.ICEBERG)

        # Simulate connecting to new compute by creating new instance
        new_capabilities = DBRCapabilities(dbr_version=(16, 2))
        assert new_capabilities.has_capability(DBRCapability.ICEBERG)


class TestCapabilitySpecs:
    """Test capability specification functionality."""

    def test_all_capabilities_have_specs(self):
        """Test that all capability enums have specifications."""
        for capability in DBRCapability:
            assert capability in DBRCapabilities.CAPABILITY_SPECS
            spec = DBRCapabilities.CAPABILITY_SPECS[capability]
            assert isinstance(spec, CapabilitySpec)
            assert spec.min_version is not None

    def test_version_requirements(self):
        """Test that version requirements are correctly specified."""
        specs = DBRCapabilities.CAPABILITY_SPECS

        # Check known version requirements
        assert specs[DBRCapability.TIMESTAMPDIFF].min_version == (10, 4)
        assert specs[DBRCapability.ICEBERG].min_version == (14, 3)
        assert specs[DBRCapability.COMMENT_ON_COLUMN].min_version == (16, 1)
        assert specs[DBRCapability.JSON_COLUMN_METADATA].min_version == (16, 2)

    def test_sql_warehouse_support_flags(self):
        """Test that SQL warehouse support is correctly specified."""
        specs = DBRCapabilities.CAPABILITY_SPECS

        # Most features are supported on SQL warehouses
        assert specs[DBRCapability.TIMESTAMPDIFF].sql_warehouse_supported
        assert specs[DBRCapability.ICEBERG].sql_warehouse_supported

        # But some are not yet available
        assert not specs[DBRCapability.STREAMING_TABLE_JSON_METADATA].sql_warehouse_supported
