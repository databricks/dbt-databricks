from collections.abc import Callable
import functools
import inspect
import re
from typing import Any
from typing import TYPE_CHECKING
from typing import TypeVar

from dbt.adapters.base import BaseAdapter
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES
from dbt_common.exceptions import DbtRuntimeError
from jinja2 import Undefined

if TYPE_CHECKING:
    from agate import Row
    from agate import Table


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
        return f"{sql[: m.start()]} ({redacted}){sql[m.end():]}"
    else:
        return sql


def remove_undefined(v: Any) -> Any:
    return None if isinstance(v, Undefined) else v


def undefined_proof(cls: type[A]) -> type[A]:
    for name in cls._available_:
        func = getattr(cls, name)
        if not callable(func):
            continue
        try:
            static_attr = inspect.getattr_static(cls, name)
            isstatic = isinstance(static_attr, staticmethod)
            isclass = isinstance(static_attr, classmethod)
        except AttributeError:
            isstatic = False
            isclass = False
        wrapped_function = _wrap_function(func.__func__ if isclass else func)
        setattr(
            cls,
            name,
            (
                staticmethod(wrapped_function)
                if isstatic
                else classmethod(wrapped_function) if isclass else wrapped_function
            ),
        )

    return cls


def _wrap_function(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        new_args = [remove_undefined(arg) for arg in args]
        new_kwargs = {key: remove_undefined(value) for key, value in kwargs.items()}
        return func(*new_args, **new_kwargs)

    return wrapper


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
