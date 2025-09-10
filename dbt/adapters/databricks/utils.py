import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from dbt_common.exceptions import DbtRuntimeError
from jinja2 import Undefined

from dbt.adapters.base import BaseAdapter
from dbt.adapters.databricks.logging import logger
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES

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
