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
    INSERT_BY_NAME = "insert_by_name"
    REPLACE_ON = "replace_on"


@dataclass
class CapabilitySpec:
    """Specification for a DBR capability."""

    min_version: tuple[int, int]
    sql_warehouse_supported: bool = True


class DBRCapabilities:
    """
    Manages DBR version capabilities for a specific compute resource.

    This class is constructed with compute information and provides
    capability checks based on that compute's characteristics.
    """

    CAPABILITY_SPECS = {
        DBRCapability.TIMESTAMPDIFF: CapabilitySpec(
            min_version=(10, 4),
        ),
        DBRCapability.ICEBERG: CapabilitySpec(
            min_version=(14, 3),
        ),
        DBRCapability.COMMENT_ON_COLUMN: CapabilitySpec(
            min_version=(16, 1),
        ),
        DBRCapability.JSON_COLUMN_METADATA: CapabilitySpec(
            min_version=(16, 2),
        ),
        DBRCapability.STREAMING_TABLE_JSON_METADATA: CapabilitySpec(
            min_version=(17, 1),
            sql_warehouse_supported=False,
        ),
        DBRCapability.INSERT_BY_NAME: CapabilitySpec(
            min_version=(12, 2),
        ),
        DBRCapability.REPLACE_ON: CapabilitySpec(
            min_version=(17, 1),
        ),
    }

    def __init__(
        self,
        dbr_version: Optional[tuple[int, int]] = None,
        is_sql_warehouse: bool = False,
    ) -> None:
        """
        Initialize the capabilities manager for a specific compute resource.

        Args:
            dbr_version: The DBR version tuple (major, minor)
            is_sql_warehouse: Whether this is a SQL warehouse
        """
        self.dbr_version = dbr_version
        self.is_sql_warehouse = is_sql_warehouse

    def has_capability(self, capability: DBRCapability) -> bool:
        """
        Check if this compute resource has a capability.

        Args:
            capability: The capability to check

        Returns:
            True if the capability is available, False otherwise
        """
        return self._compute_capability(capability)

    def _compute_capability(self, capability: DBRCapability) -> bool:
        """Compute whether a capability is available for this compute."""
        spec = self.CAPABILITY_SPECS.get(capability)
        if not spec:
            return False

        if self.is_sql_warehouse:
            if not spec.sql_warehouse_supported:
                return False
            return True

        if self.dbr_version is None:
            return False

        return self.dbr_version >= spec.min_version

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
