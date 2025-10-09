from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Optional, TypeVar
from uuid import uuid4

from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.base import ConstraintSupport
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
T = TypeVar("T", bound="TypedConstraint")


@dataclass
class TypedConstraint(ModelLevelConstraint, ABC):
    """Constraint that enforces type because it has render logic"""

    str_type: ClassVar[str]

    @classmethod
    def __post_deserialize__(cls: type[T], obj: T) -> T:
        assert obj.type == cls.str_type, "Mismatched constraint type"
        obj._validate()
        return obj

    def _validate(self) -> None:
        return

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
    def _render_error(self, missing: list[list[str]]) -> DbtValidationError:
        fields = " or ".join(["(" + ", ".join([f"'{e}'" for e in x]) + ")" for x in missing])
        name = self.name or ""
        return DbtValidationError(
            f"{self.str_type} constraint '{name}' is missing required field(s): {fields}"
        )

    # Enables set equality checks, especially for convenient unit testing
    def __hash__(self) -> int:
        # Create a tuple of all the fields that should be used for equality comparison
        fields = (
            self.type,
            self.name,
            tuple(self.columns) if self.columns else None,
            self.expression,
            self.to,
            tuple(self.to_columns) if self.to_columns else None,
        )
        return hash(fields)

    def __eq__(self, other: Any) -> bool:
        """Override equality to only compare fields used in hash calculation.

        This ensures hash/equality contract is maintained and prevents issues
        with set operations when warn_unenforced/warn_unsupported differ.
        """
        return self.__hash__() == other.__hash__()


class CustomConstraint(TypedConstraint):
    str_type = "custom"

    def _validate(self) -> None:
        if self.expression is None:
            raise self._render_error([["expression"]])

    def _render_suffix(self) -> str:
        return self.expression or ""


class PrimaryKeyConstraint(TypedConstraint):
    str_type = "primary_key"

    def _validate(self) -> None:
        if not self.columns:
            raise self._render_error([["columns"]])

    def _render_suffix(self) -> str:
        suffix = f"PRIMARY KEY ({', '.join(self.columns)})"
        if self.expression:
            suffix += f" {self.expression}"
        return suffix


class ForeignKeyConstraint(TypedConstraint):
    str_type = "foreign_key"

    def _validate(self) -> None:
        if not self.columns or (not (self.to_columns and self.to) and not self.expression):
            raise self._render_error(
                [["columns", "to", "to_columns"], ["columns", "expression"]],
            )

    def _render_suffix(self) -> str:
        suffix = f"FOREIGN KEY ({', '.join(self.columns)})"
        if self.expression:
            suffix += f" {self.expression}"
        else:
            suffix += f" REFERENCES {self.to} ({', '.join(self.to_columns)})"
        return suffix


class CheckConstraint(TypedConstraint):
    str_type = "check"

    def _validate(self) -> None:
        if not self.name:
            self.name: str = f"chk_{str(uuid4()).split('-')[0]}"
        if not self.expression:
            raise self._render_error([["expression"]])

    def _render_suffix(self) -> str:
        if self.expression[0] != "(" or self.expression[-1] != ")":
            return f"CHECK ({self.expression})"
        else:
            return f"CHECK {self.expression}"


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


def process_constraint(constraint: TypedConstraint) -> Optional[str]:
    if validate_constraint(constraint):
        return constraint.render()
    return None


def validate_constraint(constraint: ColumnLevelConstraint) -> bool:
    # Custom constraints are always supported
    if constraint.type == ConstraintType.custom:
        return True

    supported = is_supported(constraint)

    if constraint.warn_unsupported and not supported:
        warn_or_error(
            ConstraintNotSupported(constraint=constraint.type.value, adapter="DatabricksAdapter")
        )
    elif constraint.warn_unenforced and not is_enforced(constraint):
        warn_or_error(
            ConstraintNotEnforced(constraint=constraint.type.value, adapter="DatabricksAdapter")
        )

    return supported


def parse_constraints(
    model_columns: list[dict[str, Any]], model_constraints: list[dict[str, Any]]
) -> tuple[set[str], list[TypedConstraint]]:
    not_nulls, constraints_from_columns = parse_column_constraints(model_columns)
    not_nulls_from_models, constraints_from_models = parse_model_constraints(model_constraints)

    return not_nulls.union(
        not_nulls_from_models
    ), constraints_from_columns + constraints_from_models


def parse_column_constraints(
    model_columns: list[dict[str, Any]],
) -> tuple[set[str], list[TypedConstraint]]:
    column_names: set[str] = set()
    constraints: list[TypedConstraint] = []
    for column in model_columns:
        for constraint in column.get("constraints", []):
            if constraint["type"] == ConstraintType.unique:
                raise DbtValidationError("Unique constraints are not supported on Databricks")
            if constraint["type"] == ConstraintType.not_null:
                column_names.add(column["name"])
            else:
                constraint["columns"] = [
                    f"`{column['name']}`" if column.get("quote") else column["name"]
                ]
                constraints.append(parse_constraint(constraint))

    return column_names, constraints


def parse_model_constraints(
    model_constraints: list[dict[str, Any]],
) -> tuple[set[str], list[TypedConstraint]]:
    column_names: set[str] = set()
    constraints: list[TypedConstraint] = []
    for constraint in model_constraints:
        if constraint["type"] == ConstraintType.unique:
            raise DbtValidationError("Unique constraints are not supported on Databricks")
        if constraint["type"] == ConstraintType.not_null:
            if not constraint.get("columns"):
                raise DbtValidationError("not_null constraint on model must have 'columns' defined")
            column_names.update(constraint["columns"])
        else:
            constraints.append(parse_constraint(constraint))

    return column_names, constraints


CONSTRAINT_TYPE_MAP = {
    "primary_key": PrimaryKeyConstraint,
    "foreign_key": ForeignKeyConstraint,
    "check": CheckConstraint,
    "custom": CustomConstraint,
}


def parse_constraint(
    raw_constraint: dict[str, Any], type_map: dict[str, type[TypedConstraint]] = CONSTRAINT_TYPE_MAP
) -> TypedConstraint:
    try:
        klass = type_map[raw_constraint["type"]]
        klass.validate(raw_constraint)
        return klass.from_dict(raw_constraint)
    except Exception:
        raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")
