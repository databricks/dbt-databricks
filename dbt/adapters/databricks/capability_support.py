"""
Integration layer for DBR capabilities with the DatabricksAdapter.

This module provides mixins and utilities to integrate the capability system
with the existing adapter infrastructure.
"""

from functools import wraps
from typing import Any, Callable, Optional

from dbt.adapters.databricks.dbr_capabilities import DBRCapabilities, DBRCapability


class CapabilitySupport:
    """
    Mixin for adding capability support to the DatabricksAdapter.

    Note: This is a placeholder mixin. The actual capability checking is done
    through the connection object in impl.py. This class exists for potential
    future use and to document the capability API.
    """

    def __init__(self) -> None:
        self._capabilities: Optional[DBRCapabilities] = None

    @property
    def capabilities(self) -> DBRCapabilities:
        """
        Lazy-initialize and cache the capabilities object.

        This is initialized on first access to ensure the connection
        information is available.
        """
        if self._capabilities is None:
            self._capabilities = DBRCapabilities()
        return self._capabilities

    def has_capability(self, capability: DBRCapability) -> bool:
        """
        Check if a capability is available.

        Note: This method should be overridden by the adapter to pass
        compute-specific information to the capabilities system.
        """
        return False

    def require_capability(
        self, capability: DBRCapability, feature_name: Optional[str] = None
    ) -> None:
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


def with_capability(capability: DBRCapability, fallback: Any = None) -> Callable:
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

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if self.has_capability(capability):
                return func(self, *args, **kwargs)
            elif callable(fallback):
                return fallback(self, *args, **kwargs)
            else:
                return fallback

        return wrapper

    return decorator


def capability_guard(capability: DBRCapability, skip_if_missing: bool = True) -> Callable:
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

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if self.has_capability(capability):
                return func(self, *args, **kwargs)
            elif skip_if_missing:
                return None  # Skip execution
            else:
                self.require_capability(capability, f"Operation {func.__name__}")

        return wrapper

    return decorator
