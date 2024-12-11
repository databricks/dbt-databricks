from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, ClassVar, Optional, TypeVar

from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.base import ConstraintSupport
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.logging import logger
from dbt.adapters.events.types import ConstraintNotEnforced, ConstraintNotSupported

# Support constants
CONSTRAINT_SUPPORT = {
    ConstraintType.check: ConstraintSupport.ENFORCED,
    ConstraintType.not_null: ConstraintSupport.ENFORCED,
    ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
    ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
    ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
}

# Types
"""Generic type variable for constraints."""
T = TypeVar("T", bound=ColumnLevelConstraint)

"""Function type for checking constraint support."""
SupportFunc = Callable[[T], bool]


class TypedConstraint(ModelLevelConstraint, ABC):
    """Constraint that enforces type because it has render logic"""

    str_type: ClassVar[str]

    @classmethod
    def validate(cls, raw_constraint: dict[str, Any]) -> None:
        super().validate(raw_constraint)
        assert raw_constraint.get("type") == cls.str_type

    def render(self) -> str:
        return f"{self._render_prefix()}{self._render_suffix()}"

    def _render_prefix(self) -> str:
        if self.name:
            return f"CONSTRAINT {self.name} "
        return ""

    @abstractmethod
    def _render_suffix(self) -> str:
        pass

    @classmethod
    def _render_error(cls, name: str, missing: list[list[str]]) -> DbtValidationError:
        fields = " or ".join(["(" + ", ".join([f"'{e}'" for e in x]) + ")" for x in missing])
        return DbtValidationError(
            f"{cls.str_type} constraint '{name}' is missing required field(s): {fields}"
        )


class CustomConstraint(TypedConstraint):
    str_type = "custom"

    @classmethod
    def validate(cls, raw_constraint: dict[str, Any]) -> None:
        super().validate(raw_constraint)
        if raw_constraint.get("expression") is None:
            raise cls._render_error(raw_constraint.get("name", ""), [["expression"]])

    def _render_suffix(self) -> str:
        return self.expression or ""


class PrimaryKeyConstraint(TypedConstraint):
    str_type = "primary_key"

    @classmethod
    def validate(cls, raw_constraint: dict[str, Any]) -> None:
        super().validate(raw_constraint)
        if raw_constraint.get("columns", []) == [] and not raw_constraint.get("expression"):
            raise cls._render_error(raw_constraint.get("name", ""), [["columns"], ["expression"]])

    def render_suffix(self) -> str:
        suffix = f"PRIMARY KEY ({', '.join(self.columns)})"
        if self.expression:
            suffix += f" {self.expression}"
        return suffix


class ForeignKeyConstraint(TypedConstraint):
    str_type = "foreign_key"

    @classmethod
    def validate(cls, raw_constraint: dict[str, Any]) -> None:
        super().validate(raw_constraint)
        columns = raw_constraint.get("columns", [])
        to = raw_constraint.get("to")
        to_columns = raw_constraint.get("to_columns", [])
        if columns == [] or (to_columns == [] and not to and not raw_constraint.get("expression")):
            raise cls._render_error(
                raw_constraint.get("name", ""),
                [["columns", "to", "to_columns"], ["columns", "expression"]],
            )

    def render_suffix(self) -> str:
        suffix = f"FOREIGN KEY ({', '.join(self.columns)})"
        if self.expression:
            suffix += f" {self.expression}"
        else:
            suffix += f" REFERENCES {self.to} ({', '.join(self.to_columns)})"
        return suffix


class CheckConstraint(TypedConstraint):
    str_type = "check"

    @classmethod
    def validate(cls, raw_constraint: dict[str, Any]) -> None:
        super().validate(raw_constraint)
        if raw_constraint.get("expression") is None:
            raise cls._render_error(raw_constraint.get("name", ""), [["expression"]])

    def render_suffix(self) -> str:
        return f"CHECK ({self.expression})"


# Base support and enforcement
def is_supported(constraint: ColumnLevelConstraint) -> bool:
    if constraint.type == ConstraintType.custom:
        return True
    if constraint.type in CONSTRAINT_SUPPORT:
        return CONSTRAINT_SUPPORT[constraint.type] != ConstraintSupport.NOT_SUPPORTED
    return False


def is_enforced(constraint: ColumnLevelConstraint) -> bool:
    return constraint.type in CONSTRAINT_SUPPORT and CONSTRAINT_SUPPORT[constraint.type] not in [
        ConstraintSupport.NOT_ENFORCED,
        ConstraintSupport.NOT_SUPPORTED,
    ]


def process_constraint(constraint: TypedConstraint, support_func: SupportFunc) -> Optional[str]:
    if validate_constraint(constraint, support_func):
        return constraint.render()
    return None


def validate_constraint(constraint: T, support_func: SupportFunc) -> bool:
    # Custom constraints are always supported
    if constraint.type == ConstraintType.custom:
        return True

    supported = support_func(constraint)

    if constraint.warn_unsupported and not supported:
        warn_or_error(
            ConstraintNotSupported(constraint=constraint.type.value, adapter="DatabricksAdapter")
        )
    elif constraint.warn_unenforced and not is_enforced(constraint):
        warn_or_error(
            ConstraintNotEnforced(constraint=constraint.type.value, adapter="DatabricksAdapter")
        )

    return supported


def supported_for(constraint: T, support_func: SupportFunc, warning: str) -> bool:
    if is_supported(constraint) and not support_func(constraint):
        logger.warning(warning.format(type=constraint.type))

    return is_supported(constraint) and support_func(constraint)


# ColumnLevelConstraint specialization
def parse_column_constraint(
    column: DatabricksColumn, raw_constraint: dict[str, Any]
) -> ColumnLevelConstraint:
    if raw_constraint.get("type") != "not_null":
        raw_constraint["columns"] = [column.name]

    # Convert to model constraint since column constraints don't work well in multiples
    raw_constraint["columns"] = [column.name]
    return parse_constraint(raw_constraint, CONSTRAINT_TYPE_MAP)


COLUMN_WARNING = (
    "While constraint of type {type} is supported for models, it is not supported for columns."
)

supported_for_columns = partial(
    supported_for, support_func=lambda x: is_supported(x), warning=COLUMN_WARNING
)


def process_column_constraint(constraint: TypedConstraint) -> Optional[str]:
    return process_constraint(constraint, supported_for_columns)


CONSTRAINT_TYPE_MAP = {
    "primary_key": PrimaryKeyConstraint,
    "foreign_key": ForeignKeyConstraint,
    "check": CheckConstraint,
    "custom": CustomConstraint,
    "not_null": ColumnLevelConstraint,
    "unique": ColumnLevelConstraint,
}


def parse_constraint(type_map: dict[str, type[T]], raw_constraint: dict[str, Any]) -> T:
    try:
        klass = type_map[raw_constraint["type"]]
        klass.validate(raw_constraint)
        return klass.from_dict(raw_constraint)
    except Exception:
        raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")


MODEL_WARNING = (
    "While constraint of type {type} is supported for columns, it is not supported for models."
)


supported_for_models = partial(
    supported_for, support_func=lambda x: x.type != ConstraintType.not_null, warning=MODEL_WARNING
)


def process_model_constraint(constraint: TypedConstraint) -> Optional[str]:
    return process_constraint(constraint, supported_for_models)
