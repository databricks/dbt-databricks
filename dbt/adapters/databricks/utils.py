import functools
import inspect
import re
from typing import Any, Callable, Type, TypeVar

from dbt.adapters.base import BaseAdapter
from jinja2.runtime import Undefined


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


def undefined_proof(cls: Type[A]) -> Type[A]:
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
                else classmethod(wrapped_function)
                if isclass
                else wrapped_function
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
