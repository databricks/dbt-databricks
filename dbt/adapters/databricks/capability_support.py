"""
Integration layer for DBR capabilities with the DatabricksAdapter.

This module provides mixins and utilities to integrate the capability system
with the existing adapter infrastructure.
"""
from typing import TYPE_CHECKING, Optional
from functools import wraps

from dbt.adapters.databricks.dbr_capabilities import DBRCapabilities, DBRCapability

if TYPE_CHECKING:
    from dbt.adapters.databricks.impl import DatabricksAdapter


class CapabilitySupport:
    """Mixin for adding capability support to the DatabricksAdapter."""

    def __init__(self):
        self._capabilities: Optional[DBRCapabilities] = None

    @property
    def capabilities(self) -> DBRCapabilities:
        """
        Lazy-initialize and cache the capabilities object.

        This is initialized on first access to ensure the connection
        information is available.
        """
        if self._capabilities is None:
            self._initialize_capabilities()
        return self._capabilities

    def _initialize_capabilities(self) -> None:
        """Initialize the capabilities based on current connection."""
        # Get version from connection
        dbr_version = None
        is_sql_warehouse = False
        is_unity_catalog = False

        if hasattr(self, 'connections') and self.connections:
            # Get DBR version from handle
            try:
                handle = self.connections.get_thread_connection().handle
                if handle and hasattr(handle, 'dbr_version'):
                    dbr_version = handle.dbr_version
                    is_sql_warehouse = handle.is_sql_warehouse
            except Exception:
                pass  # Not connected yet

            # Check if Unity Catalog from connection config
            try:
                if hasattr(self.connections, 'profile'):
                    catalog = self.connections.profile.credentials.catalog
                    is_unity_catalog = catalog and catalog != "hive_metastore"
            except Exception:
                pass

        # Check for configuration overrides
        capability_overrides = {}
        if hasattr(self, 'config') and self.config:
            # Check for skip_metadata_queries flag
            if self.config.get('skip_metadata_queries', False):
                capability_overrides[DBRCapability.TAGS] = False
                capability_overrides[DBRCapability.COLUMN_TAGS] = False
                capability_overrides[DBRCapability.CONSTRAINTS] = False
                capability_overrides[DBRCapability.COLUMN_MASKS] = False

            # Check for specific capability overrides
            capabilities_config = self.config.get('dbr_capabilities', {})
            for cap_name, enabled in capabilities_config.items():
                try:
                    cap = DBRCapability(cap_name)
                    capability_overrides[cap] = enabled
                except ValueError:
                    pass  # Invalid capability name, ignore

        self._capabilities = DBRCapabilities(
            dbr_version=dbr_version,
            is_sql_warehouse=is_sql_warehouse,
            is_unity_catalog=is_unity_catalog,
            capability_overrides=capability_overrides,
        )

    def has_capability(self, capability: DBRCapability) -> bool:
        """Check if a capability is available."""
        return self.capabilities.has_capability(capability)

    def require_capability(self, capability: DBRCapability, feature_name: str = None):
        """
        Raise an error if a capability is not available.

        Args:
            capability: The required capability
            feature_name: Human-readable feature name for error message
        """
        if not self.has_capability(capability):
            feature_name = feature_name or capability.value
            min_version = self.capabilities.get_required_version(capability)
            raise RuntimeError(
                f"{feature_name} requires {min_version}. "
                f"Current connection does not meet this requirement."
            )


def with_capability(capability: DBRCapability, fallback=None):
    """
    Decorator to conditionally execute a method based on capability.

    Args:
        capability: Required capability
        fallback: Optional fallback value or callable if capability is not available

    Example:
        @with_capability(DBRCapability.JSON_COLUMN_METADATA, fallback=_legacy_get_columns)
        def get_columns_json(self, relation):
            # New JSON-based implementation
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.has_capability(capability):
                return func(self, *args, **kwargs)
            elif callable(fallback):
                return fallback(self, *args, **kwargs)
            else:
                return fallback
        return wrapper
    return decorator


def capability_guard(capability: DBRCapability, skip_if_missing: bool = True):
    """
    Decorator to guard a code block based on capability.

    Args:
        capability: Required capability
        skip_if_missing: If True, skip execution; if False, raise error

    Example:
        @capability_guard(DBRCapability.COLUMN_TAGS)
        def apply_column_tags(self, relation, tags):
            # This will be skipped if column tags are not supported
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.has_capability(capability):
                return func(self, *args, **kwargs)
            elif skip_if_missing:
                return None  # Skip execution
            else:
                self.require_capability(capability, f"Operation {func.__name__}")
        return wrapper
    return decorator