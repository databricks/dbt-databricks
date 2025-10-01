"""
DBR (Databricks Runtime) capability management system.

This module provides a centralized way to check for DBR version-gated features,
replacing scattered version comparisons throughout the codebase with named capabilities.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


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
    min_version: tuple[int, int]  # (major, minor)
    requires_unity_catalog: bool = False
    sql_warehouse_supported: bool = True
    enabled_by_default: bool = True


class DBRCapabilities:
    """
    Manages DBR version capabilities for a specific compute resource.

    This class is constructed with compute information and provides
    capability checks based on that compute's characteristics.
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

    def __init__(
        self,
        dbr_version: Optional[tuple[int, int]] = None,
        is_sql_warehouse: bool = False,
        is_unity_catalog: bool = False,
        capability_overrides: Optional[dict[DBRCapability, bool]] = None,
    ) -> None:
        """
        Initialize the capabilities manager for a specific compute resource.

        Args:
            dbr_version: The DBR version tuple (major, minor)
            is_sql_warehouse: Whether this is a SQL warehouse
            is_unity_catalog: Whether this uses Unity Catalog
            capability_overrides: Manual overrides for specific capabilities
        """
        self.dbr_version = dbr_version
        self.is_sql_warehouse = is_sql_warehouse
        self.is_unity_catalog = is_unity_catalog
        self._capability_overrides = capability_overrides or {}
        # Cache computed results
        self._capability_cache: dict[DBRCapability, bool] = {}

    def has_capability(self, capability: DBRCapability) -> bool:
        """
        Check if this compute resource has a capability.

        Args:
            capability: The capability to check

        Returns:
            True if the capability is available, False otherwise
        """
        # Check override first
        if capability in self._capability_overrides:
            return self._capability_overrides[capability]

        # Check cache
        if capability in self._capability_cache:
            return self._capability_cache[capability]

        # Compute and cache result
        result = self._compute_capability(capability)
        self._capability_cache[capability] = result
        return result

    def _compute_capability(self, capability: DBRCapability) -> bool:
        """Compute whether a capability is available for this compute."""
        spec = self.CAPABILITY_SPECS.get(capability)
        if not spec:
            return False

        # Check if disabled by default
        if not spec.enabled_by_default:
            return False

        # SQL warehouse support check
        if self.is_sql_warehouse:
            if not spec.sql_warehouse_supported:
                return False
            # SQL warehouses are assumed to have latest features when supported
            return True

        # Unity Catalog requirement check
        if spec.requires_unity_catalog and not self.is_unity_catalog:
            return False

        # Check DBR version
        if self.dbr_version is None:
            # No version info means we assume no capabilities
            return False

        return self.dbr_version >= spec.min_version

    def set_capability(self, capability: DBRCapability, enabled: bool) -> None:
        """
        Set a capability override and clear its cache.

        Args:
            capability: The capability to override
            enabled: Whether the capability should be enabled
        """
        self._capability_overrides[capability] = enabled
        # Clear cache for this capability
        if capability in self._capability_cache:
            del self._capability_cache[capability]

    @classmethod
    def get_required_version(cls, capability: DBRCapability) -> str:
        """Get the minimum required version string for a capability."""
        spec = cls.CAPABILITY_SPECS.get(capability)
        if not spec:
            return "Unknown"
        return f"DBR {spec.min_version[0]}.{spec.min_version[1]}+"

    def enabled_capabilities(self) -> set[DBRCapability]:
        """Get all currently enabled capabilities for this compute."""
        return {capability for capability in DBRCapability if self.has_capability(capability)}
