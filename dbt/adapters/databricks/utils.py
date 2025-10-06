import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from dbt.adapters.base import BaseAdapter
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES
from dbt_common.exceptions import DbtRuntimeError
from jinja2 import Undefined

from databricks.sdk.service.catalog import ColumnInfo, TableInfo, TableType
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    from agate import Row, Table


A = TypeVar("A", bound=BaseAdapter)


CREDENTIAL_IN_COPY_INTO_REGEX = re.compile(
    r"(?<=credential)\s*?\((\s*?'\w*?'\s*?=\s*?'.*?'\s*?(?:,\s*?'\w*?'\s*?=\s*?'.*?'\s*?)*?)\)"
)


def redact_credentials(sql: str) -> str:
    redacted = _redact_credentials_in_copy_into(sql)
    return redacted


def _redact_credentials_in_copy_into(sql: str) -> str:
    m = CREDENTIAL_IN_COPY_INTO_REGEX.search(sql, re.MULTILINE)
    if m:
        redacted = ", ".join(
            f"{key.strip()} = '[REDACTED]'"
            for key, _ in (pair.strip().split("=", 1) for pair in m.group(1).split(","))
        )
        return f"{sql[: m.start()]} ({redacted}){sql[m.end() :]}"
    else:
        return sql


def remove_undefined(v: Any) -> Any:
    return None if isinstance(v, Undefined) else v


def remove_ansi(line: str) -> str:
    ansi_escape = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", line)


def get_first_row(results: "Table") -> "Row":
    if len(results.rows) == 0:
        # Lazy load to improve CLI startup time
        from agate import Row

        return Row(values=set())
    return results.rows[0]


def check_not_found_error(errmsg: str) -> bool:
    new_error = "[SCHEMA_NOT_FOUND]" in errmsg
    old_error = re.match(r".*(Database).*(not found).*", errmsg, re.DOTALL)
    found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
    return new_error or old_error is not None or any(found_msgs)


T = TypeVar("T")


def handle_missing_objects(exec: Callable[[], T], default: T) -> T:
    try:
        return exec()
    except DbtRuntimeError as e:
        errmsg = getattr(e, "msg", "")
        if check_not_found_error(errmsg):
            return default
        raise e


def quote(name: str) -> str:
    return f"`{name}`" if "`" not in name else name


ExceptionToStrOp = Callable[[Exception], str]


def handle_exceptions_as_warning(op: Callable[[], None], log_gen: ExceptionToStrOp) -> None:
    try:
        op()
    except Exception as e:
        logger.warning(log_gen(e))


def is_cluster_http_path(http_path: str, cluster_id: Optional[str]) -> bool:
    if "/warehouses/" in http_path:
        return False
    if "/protocolv1/" in http_path:
        return True
    return cluster_id is not None


# Unity Catalog SDK type mapping utilities

# Table types that map to 'table' in dbt (vs view, streaming_table, etc.)
MANAGED_TABLE_TYPES = frozenset(
    {
        "EXTERNAL",
        "MANAGED",
        "MANAGED_SHALLOW_CLONE",
        "EXTERNAL_SHALLOW_CLONE",
    }
)


def map_table_info_to_relation_info(table: "TableInfo") -> Any:
    """
    Convert SDK TableInfo to dbt DatabricksRelationInfo.

    Imports DatabricksRelationInfo locally to avoid circular dependency.
    """
    from dbt.adapters.databricks.impl import DatabricksRelationInfo

    table_type = _map_sdk_table_type_to_string(table.table_type)
    databricks_table_type = _get_databricks_table_type_from_sdk(table.table_type)
    file_format = table.data_source_format.value.lower() if table.data_source_format else None

    return DatabricksRelationInfo(
        table.name or "",
        table_type,
        file_format,
        table.owner,
        databricks_table_type,
    )


def _extract_table_type_string(table_type: Optional["TableType"]) -> str:
    """Extract string value from TableType enum, handling None gracefully."""
    if not table_type:
        return ""
    return getattr(table_type, "value", str(table_type))


def _map_sdk_table_type_to_string(table_type: Optional["TableType"]) -> str:
    """
    Map SDK TableType enum to string format expected by dbt.

    SDK returns: MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, etc.
    We need: 'table', 'view', 'materialized_view', 'streaming_table'
    """
    type_str = _extract_table_type_string(table_type)
    if not type_str:
        return "table"

    if type_str.upper() in MANAGED_TABLE_TYPES:
        return "table"

    return type_str.lower()


def _get_databricks_table_type_from_sdk(table_type: Optional["TableType"]) -> Optional[str]:
    """
    Get the databricks-specific table type (external, managed, etc.).

    Returns the lowercased type for EXTERNAL, MANAGED, etc., or None for other types.
    """
    type_str = _extract_table_type_string(table_type)
    if not type_str:
        return None

    if type_str.upper() in MANAGED_TABLE_TYPES:
        return type_str.lower()

    return None


def map_column_info_to_databricks_column(column_info: "ColumnInfo") -> Any:
    """
    Convert SDK ColumnInfo to dbt DatabricksColumn.

    Uses type_json when available for complete type information (handles complex types
    like deeply nested structs), otherwise falls back to type_text.

    Imports DatabricksColumn locally to avoid circular dependency.
    """
    import json

    from dbt.adapters.databricks.column import DatabricksColumn

    # Prefer type_json for complete type information (handles complex types correctly)
    if column_info.type_json:
        try:
            type_info = json.loads(column_info.type_json)
            dtype = DatabricksColumn._parse_type_from_json(type_info)
        except (json.JSONDecodeError, Exception) as e:
            # Fall back to type_text if JSON parsing fails
            logger.debug(
                f"Failed to parse type_json for column {column_info.name}: {e}. "
                f"Falling back to type_text={column_info.type_text}"
            )
            dtype = column_info.type_text or ""
    else:
        # Fall back to type_text if type_json is not available
        dtype = column_info.type_text or ""
        if not dtype:
            logger.warning(
                f"Column {column_info.name} has no type_json or type_text. "
                f"This may indicate incomplete SDK metadata."
            )

    return DatabricksColumn(
        column=column_info.name or "",
        dtype=dtype,
        comment=column_info.comment,
    )


def map_table_info_to_column_masks_rows(table_info: "TableInfo") -> list[list[Any]]:
    """
    Convert SDK TableInfo column masks to agate-compatible rows.

    Returns list of rows in format: [column_name, mask_name, using_columns]
    This format matches what information_schema.column_masks query returns.
    """
    rows: list[list[Any]] = []
    if not table_info.columns:
        return rows

    for column in table_info.columns:
        if column.mask:
            using_columns = ",".join(column.mask.using_column_names or [])
            rows.append([column.name, column.mask.function_name, using_columns or None])

    return rows


def map_table_info_to_primary_key_constraints_rows(
    table_info: "TableInfo",
) -> list[list[Any]]:
    """
    Convert SDK TableInfo constraints to primary key constraint rows.

    Returns list of rows in format: [constraint_name, column_name]
    This format matches what fetch_primary_key_constraints query returns.
    """
    rows: list[list[Any]] = []
    if not table_info.table_constraints:
        return rows

    for constraint in table_info.table_constraints:
        if constraint.primary_key_constraint:
            pk = constraint.primary_key_constraint
            constraint_name = pk.name or "PRIMARY"
            # Each column gets its own row
            for column_name in pk.child_columns or []:
                rows.append([constraint_name, column_name])

    return rows


def map_table_info_to_foreign_key_constraints_rows(
    table_info: "TableInfo",
) -> list[list[Any]]:
    """
    Convert SDK TableInfo constraints to foreign key constraint rows.

    Returns list of rows in format:
    [constraint_name, from_column, to_catalog, to_schema, to_table, to_column]
    This format matches what fetch_foreign_key_constraints query returns.
    """
    rows: list[list[Any]] = []
    if not table_info.table_constraints:
        return rows

    for constraint in table_info.table_constraints:
        if constraint.foreign_key_constraint:
            fk = constraint.foreign_key_constraint
            constraint_name = fk.name or "FOREIGN_KEY"

            # Parse parent_table which is in format "catalog.schema.table"
            parent_parts = fk.parent_table.split(".")
            if len(parent_parts) == 3:
                to_catalog, to_schema, to_table = parent_parts
            else:
                # Fallback if format is different
                to_catalog = to_schema = to_table = fk.parent_table

            # Pair up child and parent columns
            child_cols = fk.child_columns or []
            parent_cols = fk.parent_columns or []

            for i, child_col in enumerate(child_cols):
                parent_col = parent_cols[i] if i < len(parent_cols) else ""
                rows.append(
                    [
                        constraint_name,
                        child_col,
                        to_catalog,
                        to_schema,
                        to_table,
                        parent_col,
                    ]
                )

    return rows
