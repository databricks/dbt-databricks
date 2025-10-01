"""
DBR (Databricks Runtime) capability management system.

This module provides a centralized way to check for DBR version-gated features,
replacing scattered version comparisons throughout the codebase with named capabilities.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class DBRCapability(Enum):
    """Named capabilities that depend on DBR version."""

    TIMESTAMPDIFF = "timestampdiff"
    ICEBERG = "iceberg"
    COMMENT_ON_COLUMN = "comment_on_column"
    JSON_COLUMN_METADATA = "json_column_metadata"
    STREAMING_TABLE_JSON_METADATA = "streaming_table_json_metadata"


@dataclass
class CapabilitySpec:
    """Specification for a DBR capability."""

    capability: DBRCapability
    min_version: Tuple[int, int]  # (major, minor)
    requires_unity_catalog: bool = False
    sql_warehouse_supported: bool = True
    enabled_by_default: bool = True


class DBRCapabilities:
    """
    Manages DBR version capabilities with per-compute caching.

    This class:
    1. Caches version checks per compute to avoid repeated queries
    2. Provides feature flags for version-gated functionality
    3. Allows configuration overrides for testing or compatibility
    4. Supports multiple compute resources within the same session
    """

    # Define all capability specifications
    CAPABILITY_SPECS = {
        DBRCapability.TIMESTAMPDIFF: CapabilitySpec(
            capability=DBRCapability.TIMESTAMPDIFF,
            min_version=(10, 4),
        ),
        DBRCapability.ICEBERG: CapabilitySpec(
            capability=DBRCapability.ICEBERG,
            min_version=(14, 3),
        ),
        DBRCapability.COMMENT_ON_COLUMN: CapabilitySpec(
            capability=DBRCapability.COMMENT_ON_COLUMN,
            min_version=(16, 1),
        ),
        DBRCapability.JSON_COLUMN_METADATA: CapabilitySpec(
            capability=DBRCapability.JSON_COLUMN_METADATA,
            min_version=(16, 2),
        ),
        DBRCapability.STREAMING_TABLE_JSON_METADATA: CapabilitySpec(
            capability=DBRCapability.STREAMING_TABLE_JSON_METADATA,
            min_version=(17, 1),
            sql_warehouse_supported=False,  # Not yet available in SQL warehouses
        ),
    }

    def __init__(self):
        """
        Initialize the capabilities manager.

        Note: Connection information is now passed per-capability-check to support
        multiple compute resources within the same adapter session.
        """
        # Cache by compute identifier (http_path or similar)
        self._capability_cache_by_compute: dict[str, dict[DBRCapability, bool]] = {}
        # Store compute metadata for each compute identifier
        self._compute_metadata: dict[str, dict[str, Any]] = {}

    def has_capability(
        self,
        capability: DBRCapability,
        compute_id: str,
        dbr_version: Optional[Tuple[int, int]] = None,
        is_sql_warehouse: bool = False,
        is_unity_catalog: bool = False,
        capability_overrides: Optional[dict[DBRCapability, bool]] = None,
    ) -> bool:
        """
        Check if a specific compute resource has a capability.

        Args:
            capability: The capability to check
            compute_id: Unique identifier for the compute (e.g., http_path)
            dbr_version: The DBR version tuple (major, minor) for this compute
            is_sql_warehouse: Whether this compute is a SQL warehouse
            is_unity_catalog: Whether this compute uses Unity Catalog
            capability_overrides: Manual overrides for capabilities

        This method:
        1. Checks for manual overrides first
        2. Caches results per compute to avoid repeated version comparisons
        3. Returns False for unavailable features rather than raising errors
        """
        # Check override first
        if capability_overrides and capability in capability_overrides:
            return capability_overrides[capability]

        # Get or create cache for this compute
        if compute_id not in self._capability_cache_by_compute:
            self._capability_cache_by_compute[compute_id] = {}
            # Store compute metadata for later reference
            self._compute_metadata[compute_id] = {
                "dbr_version": dbr_version,
                "is_sql_warehouse": is_sql_warehouse,
                "is_unity_catalog": is_unity_catalog,
            }

        compute_cache = self._capability_cache_by_compute[compute_id]

        # Check compute-specific cache
        if capability in compute_cache:
            return compute_cache[capability]

        # Compute capability for this specific compute
        result = self._compute_capability(
            capability, dbr_version, is_sql_warehouse, is_unity_catalog
        )
        compute_cache[capability] = result
        return result

    def _compute_capability(
        self,
        capability: DBRCapability,
        dbr_version: Optional[Tuple[int, int]],
        is_sql_warehouse: bool,
        is_unity_catalog: bool,
    ) -> bool:
        """Compute whether a capability is available for specific compute."""
        spec = self.CAPABILITY_SPECS.get(capability)
        if not spec:
            return False

        # Check if disabled by default
        if not spec.enabled_by_default:
            return False

        # SQL warehouse support check
        if is_sql_warehouse:
            if not spec.sql_warehouse_supported:
                return False
            # SQL warehouses are assumed to have latest features when supported
            return True

        # Unity Catalog requirement check
        if spec.requires_unity_catalog and not is_unity_catalog:
            return False

        # Check DBR version
        if dbr_version is None:
            # No version info means we assume no capabilities
            return False

        return dbr_version >= spec.min_version

    def set_capability(
        self, capability: DBRCapability, enabled: bool, compute_id: Optional[str] = None
    ) -> None:
        """
        Set a capability override and clear its cache.

        Args:
            capability: The capability to override
            enabled: Whether the capability should be enabled
            compute_id: Optionally set for specific compute only. If None, clears all.
        """
        if compute_id:
            # Clear cache for specific compute
            if compute_id in self._capability_cache_by_compute:
                compute_cache = self._capability_cache_by_compute[compute_id]
                if capability in compute_cache:
                    del compute_cache[capability]
        else:
            # Clear cache for all computes for this capability
            for compute_cache in self._capability_cache_by_compute.values():
                if capability in compute_cache:
                    del compute_cache[capability]

    def get_required_version(self, capability: DBRCapability) -> str:
        """Get the minimum required version string for a capability."""
        spec = self.CAPABILITY_SPECS.get(capability)
        if not spec:
            return "Unknown"
        return f"DBR {spec.min_version[0]}.{spec.min_version[1]}+"

    def clear_compute_cache(self, compute_id: str) -> None:
        """
        Clear all cached capabilities for a specific compute.

        Args:
            compute_id: The compute identifier to clear cache for
        """
        if compute_id in self._capability_cache_by_compute:
            del self._capability_cache_by_compute[compute_id]
        if compute_id in self._compute_metadata:
            del self._compute_metadata[compute_id]

    def enabled_capabilities(
        self,
        compute_id: str,
        dbr_version: Optional[Tuple[int, int]] = None,
        is_sql_warehouse: bool = False,
        is_unity_catalog: bool = False,
        capability_overrides: Optional[dict[DBRCapability, bool]] = None,
    ) -> set[DBRCapability]:
        """Get all currently enabled capabilities for a specific compute."""
        return {
            capability
            for capability in DBRCapability
            if self.has_capability(
                capability,
                compute_id,
                dbr_version,
                is_sql_warehouse,
                is_unity_catalog,
                capability_overrides,
            )
        }

