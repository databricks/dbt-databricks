import json
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from dbt.adapters.base import BaseAdapter
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES
from dbt_common.exceptions import DbtRuntimeError, DbtValidationError
from jinja2 import Undefined

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


class QueryTagsUtils:
    """Utility class for handling query tags merging and validation."""

    # Reserved query tag keys that cannot be overridden
    RESERVED_KEYS = {
        "dbt_model_name",
        "dbt_core_version",
        "dbt_databricks_version",
        "dbt_materialized",
    }

    # Maximum number of query tags allowed
    MAX_TAGS = 20

    @staticmethod
    def parse_query_tags(query_tags_str: Optional[str]) -> dict[str, str]:
        """Parse query tags from JSON string format."""
        if not query_tags_str:
            return {}

        try:
            parsed = json.loads(query_tags_str)
            if not isinstance(parsed, dict):
                raise DbtValidationError("query_tags must be a JSON object (dictionary)")

            # Validate that all values are strings
            for key, value in parsed.items():
                if not isinstance(value, str):
                    raise DbtValidationError(
                        f"query_tags values must be strings. Found {type(value).__name__} "
                        f"for key '{key}': {value}. Only string values are supported."
                    )

            # Convert keys to strings and return
            return {str(k): v for k, v in parsed.items()}
        except json.JSONDecodeError as e:
            raise DbtValidationError(f"Invalid JSON in query_tags: {e}")

    @staticmethod
    def validate_query_tags(tags: dict[str, str], source: str = "") -> None:
        """Validate query tags for reserved keys and limits."""
        source_prefix = f"{source}: " if source else ""

        # Check for reserved keys
        reserved_found = set(tags.keys()) & QueryTagsUtils.RESERVED_KEYS
        if reserved_found:
            raise DbtValidationError(
                f"{source_prefix}Cannot use reserved query tag keys: "
                f"{', '.join(sorted(reserved_found))}. "
                f"Reserved keys are: {', '.join(sorted(QueryTagsUtils.RESERVED_KEYS))}"
            )

        # Check tag limit
        if len(tags) > QueryTagsUtils.MAX_TAGS:
            raise DbtValidationError(
                f"{source_prefix}Too many query tags ({len(tags)}). "
                f"Maximum allowed is {QueryTagsUtils.MAX_TAGS}"
            )

    @staticmethod
    def merge_query_tags(
        connection_tags: dict[str, str],
        model_tags: dict[str, str],
        default_tags: dict[str, str],
    ) -> dict[str, str]:
        """
        Merge query tags with precedence: model > connection > default.
        Validates that no reserved keys are used and tag limits are respected.
        """
        # All sources are now already parsed dicts
        conn_tags = connection_tags
        model_tags_dict = model_tags
        default_tags_dict = default_tags

        # Validate each source (user-provided tags cannot use reserved keys)
        QueryTagsUtils.validate_query_tags(conn_tags, "Connection config")
        QueryTagsUtils.validate_query_tags(model_tags_dict, "Model config")

        # Merge with precedence: model > connection > default
        merged = {}
        merged.update(default_tags_dict)
        merged.update(conn_tags)
        merged.update(model_tags_dict)

        # Final validation of merged tags (only check total count, not reserved keys
        # since default tags are allowed to use reserved keys)
        if len(merged) > QueryTagsUtils.MAX_TAGS:
            raise DbtValidationError(
                f"Too many total query tags ({len(merged)}). "
                f"Maximum allowed is {QueryTagsUtils.MAX_TAGS}"
            )

        return merged

    @staticmethod
    def format_query_tags_for_databricks(tags: dict[str, str]) -> str:
        """Format query tags for Databricks session configuration (without quotes)."""
        # Format as {key:value,key:value} without quotes around keys/values
        formatted_pairs = [f"{key}:{value}" for key, value in tags.items()]
        return ",".join(formatted_pairs)
