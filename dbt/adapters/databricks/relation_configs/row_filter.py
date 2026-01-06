import csv
from io import StringIO
from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
    get_config_value,
)


class RowFilterConfig(DatabricksComponentConfig):
    """Row filter definition (function + columns).

    This class represents both the desired/existing state of a row filter AND
    the diff result. When used as a diff result:
    - should_unset=True means "remove the existing filter"
    - should_unset=False with function set means "apply this filter"
    - is_change=True indicates this is a diff that should trigger ALTER

    The is_change field is critical for streaming tables which use `diff or value`
    pattern (streaming_table.py:56). Without it, unchanged row filters would still
    trigger ALTER statements because the fallback value has a truthy function field.
    """

    # Fully qualified function name (catalog.schema.function)
    function: Optional[str] = None
    # Column names passed to the filter function
    columns: tuple[str, ...] = ()
    # True when this instance represents a diff meaning "unset/drop the filter"
    should_unset: bool = False
    # True when this represents an actual change that should trigger ALTER
    # (distinguishes diff result from `diff or value` fallback state)
    is_change: bool = False

    @staticmethod
    def _normalize_function(func: Optional[str]) -> Optional[str]:
        """Normalize function name for comparison (lowercase, strip backticks)."""
        if func is None:
            return None
        return func.lower().replace("`", "")

    @staticmethod
    def _normalize_columns(cols: tuple[str, ...]) -> tuple[str, ...]:
        """Normalize column names for comparison."""
        return tuple(c.lower().replace("`", "") for c in cols)

    def get_diff(self, other: "RowFilterConfig") -> Optional["RowFilterConfig"]:
        """Compare desired state (self) with existing state (other).

        Returns:
            - None if no changes needed
            - RowFilterConfig with should_unset=True, is_change=True if filter should be removed
            - RowFilterConfig with the filter config and is_change=True if filter should be set/updated
        """
        # Case 1: No filter desired, no filter exists -> no change
        if self.function is None and other.function is None:
            return None

        # Case 2: No filter desired, filter exists -> unset it
        if self.function is None and other.function is not None:
            return RowFilterConfig(should_unset=True, is_change=True)

        # Case 3: Filter desired, compare with existing
        if self._normalize_function(self.function) == self._normalize_function(other.function):
            if self._normalize_columns(self.columns) == self._normalize_columns(other.columns):
                return None  # No change

        # Filter is new or changed -> return new instance with is_change=True
        # (can't return self because model is frozen and we need is_change=True)
        return RowFilterConfig(
            function=self.function,
            columns=self.columns,
            is_change=True,
        )


class RowFilterProcessor(DatabricksComponentProcessor[RowFilterConfig]):
    """Processor for extracting row filter config from relations and model nodes."""

    name: ClassVar[str] = "row_filter"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> RowFilterConfig:
        """Extract existing row filter from INFORMATION_SCHEMA results."""
        table = results.get("row_filters")

        if not table or len(table.rows) == 0:
            return RowFilterConfig()

        # Handle multiple rows case (ABAC, platform bugs, etc.)
        if len(table.rows) > 1:
            filter_names = [row[3] for row in table.rows]  # filter_name is index 3
            raise ValueError(
                f"Multiple row filters found: {filter_names}. "
                f"This may indicate ABAC-derived filters or a platform issue. "
                f"dbt expects a single row filter per table."
            )

        # Unity Catalog returns one row per table (single filter constraint)
        row = table.rows[0]
        # Columns: table_catalog(0), table_schema(1), table_name(2), filter_name(3), target_columns(4)
        filter_name = row[3]  # Already fully qualified: catalog.schema.function
        target_columns = row[4]  # Comma-separated column list

        # filter_name is already fully qualified from INFORMATION_SCHEMA (catalog.schema.func)
        # Store raw - backticks are added at SQL generation time
        function = filter_name

        # Parse target_columns (handle quoted values with commas)
        columns = cls._parse_target_columns(target_columns)

        return RowFilterConfig(function=function, columns=tuple(columns))

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> RowFilterConfig:
        """Extract row filter config from dbt model node."""
        row_filter = get_config_value(relation_config, "row_filter")

        if not row_filter:
            return RowFilterConfig()

        function = row_filter.get("function")
        columns = row_filter.get("columns", [])

        if not function:
            return RowFilterConfig()

        # Normalize string to list
        if isinstance(columns, str):
            columns = [columns]

        # Validate columns is non-empty when function is set
        if not columns or len(columns) == 0:
            raise ValueError(
                f"Row filter function '{function}' requires a non-empty 'columns' value. "
                f"Example: columns: region  OR  columns: [\"region_id\", \"country_code\"]"
            )

        # Validate each column element is a non-empty string
        for i, col in enumerate(columns):
            if not isinstance(col, str) or not col.strip():
                raise ValueError(
                    f"Row filter column at index {i} must be a non-empty string. "
                    f"Got: {repr(col)}"
                )

        # Qualify function name if not already qualified
        function = cls._qualify_function_name(function, relation_config)

        return RowFilterConfig(function=function, columns=tuple(columns))

    @classmethod
    def _qualify_function_name(cls, function: str, relation_config: RelationConfig) -> str:
        """Ensure function name is fully qualified with catalog.schema.

        Handle 1-part, 2-part, 3-part names explicitly.

        IMPORTANT: This logic must stay in sync with the Jinja
        `qualify_row_filter_function()` macro. Both use the same rules:
        - 1-part: qualify with relation's database.schema
        - 2-part: reject as ambiguous
        - 3-part: use as-is
        - 4+ parts: reject
        """
        parts = function.replace("`", "").split(".")

        if len(parts) == 1:
            # Unqualified: fn -> catalog.schema.fn
            catalog = relation_config.database
            schema = relation_config.schema
            return f"{catalog}.{schema}.{parts[0]}"
        elif len(parts) == 2:
            # Ambiguous: schema.fn - reject with clear error
            raise ValueError(
                f"Row filter function '{function}' is ambiguous. "
                f"Use either unqualified name (e.g., 'my_filter') or "
                f"fully qualified name (e.g., 'catalog.schema.my_filter')."
            )
        elif len(parts) == 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        else:
            raise ValueError(
                f"Row filter function '{function}' has too many parts. "
                f"Expected format: 'catalog.schema.function_name'."
            )

    @classmethod
    def _parse_target_columns(cls, target_columns: Optional[str]) -> list[str]:
        """Parse target_columns string from INFORMATION_SCHEMA, handling quoted values with commas."""
        if not target_columns:
            return []

        # Use CSV parser to handle quoted strings with embedded commas
        # skipinitialspace=True handles space after comma: '"col1", "col2"'
        reader = csv.reader(StringIO(target_columns), skipinitialspace=True)
        for row in reader:
            return [col.strip() for col in row]
        return []
