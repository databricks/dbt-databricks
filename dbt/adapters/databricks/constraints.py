from functools import partial
from typing import Any, Callable, Optional, TypeVar

from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.base import ConstraintSupport
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

SUPPORTED_FOR_COLUMN = {
    ConstraintType.custom,
    ConstraintType.primary_key,
    ConstraintType.foreign_key,
}

# Types
"""Generic type variable for constraints."""
T = TypeVar("T", bound=ColumnLevelConstraint)

"""Function type for checking constraint support."""
SupportFunc = Callable[[T], bool]

"""Function type for rendering constraints."""
RenderFunc = Callable[[T], str]


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


# Core parsing, validation, and processing
def parse_constraint(klass: type[T], raw_constraint: dict[str, Any]) -> T:
    try:
        klass.validate(raw_constraint)
        return klass.from_dict(raw_constraint)
    except Exception:
        raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")


def process_constraint(
    constraint: T,
    support_func: SupportFunc,
    render_funcs: dict[ConstraintType, RenderFunc],
) -> Optional[str]:
    if validate_constraint(constraint, support_func):
        return render_constraint(constraint, render_funcs)

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


def render_constraint(
    constraint: T, render_funcs: dict[ConstraintType, RenderFunc]
) -> Optional[str]:
    rendered_constraint = ""

    if constraint.type in render_funcs:
        if constraint.name:
            rendered_constraint = f"CONSTRAINT {constraint.name} "
        rendered_constraint += render_funcs[constraint.type](constraint)

    rendered_constraint = rendered_constraint.strip()

    return rendered_constraint if rendered_constraint != "" else None


def supported_for(constraint: T, support_func: SupportFunc, warning: str) -> bool:
    if is_supported(constraint) and not support_func(constraint):
        logger.warning(warning.format(type=constraint.type))

    return is_supported(constraint) and support_func(constraint)


# Shared render functions
def render_error(constraint: ColumnLevelConstraint, missing: list[list[str]]) -> DbtValidationError:
    fields = " or ".join(["(" + ", ".join([f"'{e}'" for e in x]) + ")" for x in missing])
    constraint_type = constraint.type.value
    constraint_name = "" if not constraint.name else f"{constraint.name} "
    return DbtValidationError(
        f"{constraint_type} constraint {constraint_name}is missing required field(s): {fields}"
    )


def render_custom(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.custom
    if constraint.expression:
        return constraint.expression
    raise render_error(constraint, [["expression"]])


# ColumnLevelConstraint specialization
def parse_column_constraint(raw_constraint: dict[str, Any]) -> ColumnLevelConstraint:
    return parse_constraint(ColumnLevelConstraint, raw_constraint)


COLUMN_WARNING = (
    "While constraint of type {type} is supported for models, it is not supported for columns."
)

supported_for_columns = partial(
    supported_for, support_func=lambda x: x.type in SUPPORTED_FOR_COLUMN, warning=COLUMN_WARNING
)


def column_constraint_map() -> dict[ConstraintType, RenderFunc]:
    return {
        ConstraintType.primary_key: render_primary_key_for_column,
        ConstraintType.foreign_key: render_foreign_key_for_column,
        ConstraintType.custom: render_custom,
    }


def process_column_constraint(constraint: ColumnLevelConstraint) -> Optional[str]:
    return process_constraint(constraint, supported_for_columns, column_constraint_map())


def render_primary_key_for_column(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.primary_key
    rendered = "PRIMARY KEY"
    if constraint.expression:
        rendered += f" {constraint.expression}"
    return rendered


def render_foreign_key_for_column(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.foreign_key
    rendered = "FOREIGN KEY"
    if constraint.expression:
        return rendered + f" {constraint.expression}"
    elif constraint.to and constraint.to_columns:
        return rendered + f" REFERENCES {constraint.to} ({', '.join(constraint.to_columns)})"
    raise render_error(constraint, [["expression"], ["to", "to_columns"]])


MODEL_WARNING = (
    "While constraint of type {type} is supported for columns, it is not supported for models."
)


supported_for_models = partial(
    supported_for, support_func=lambda x: x.type != ConstraintType.not_null, warning=MODEL_WARNING
)


def model_constraint_map() -> dict[ConstraintType, RenderFunc]:
    return {
        ConstraintType.primary_key: render_primary_key_for_model,
        ConstraintType.foreign_key: render_foreign_key_for_model,
        ConstraintType.custom: render_custom,
        ConstraintType.check: render_check,
    }


def process_model_constraint(constraint: ModelLevelConstraint) -> Optional[str]:
    return process_constraint(constraint, supported_for_models, model_constraint_map())


def render_primary_key_for_model(constraint: ModelLevelConstraint) -> str:
    prefix = render_primary_key_for_column(constraint)
    if constraint.expression:
        return prefix
    if constraint.columns:
        return f"{prefix} ({', '.join(constraint.columns)})"
    raise render_error(constraint, [["columns"], ["expression"]])


def render_foreign_key_for_model(constraint: ModelLevelConstraint) -> str:
    assert constraint.type == ConstraintType.foreign_key
    rendered = "FOREIGN KEY"
    if constraint.columns and constraint.to and constraint.to_columns:
        columns = ", ".join(constraint.columns)
        to_columns = ", ".join(constraint.to_columns)
        return rendered + f" ({columns}) REFERENCES {constraint.to} ({to_columns})"
    elif constraint.expression:
        return rendered + " " + constraint.expression
    raise render_error(constraint, [["expression"], ["columns", "to", "to_columns"]])


def render_check(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.check
    if constraint.expression:
        return f"CHECK ({constraint.expression})"

    raise render_error(constraint, [["expression"]])
