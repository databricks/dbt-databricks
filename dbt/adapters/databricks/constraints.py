from typing import Any, Optional
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ModelLevelConstraint,
    ConstraintType,
)
from dbt_common.exceptions import DbtValidationError
from dbt_common.events.functions import warn_or_error
from dbt.adapters.databricks.impl import DatabricksAdapter, ConstraintSupport
from dbt.adapters.events.types import ConstraintNotSupported, ConstraintNotEnforced
from dbt.adapters.databricks.logging import logger

SUPPORTED_FOR_COLUMN = {
    ConstraintType.custom,
    ConstraintType.primary_key,
    ConstraintType.foreign_key,
}


def warn_if_unsupported(constraint: ColumnLevelConstraint) -> None:
    if constraint.warn_unsupported and not supported(constraint):
        warn_or_error(
            ConstraintNotSupported(
                constraint=constraint.type.value, adapter=DatabricksAdapter.type()
            )
        )


def warn_if_unenforced(constraint: ColumnLevelConstraint) -> None:
    if constraint.warn_unenforced and not enforced(constraint):
        warn_or_error(
            ConstraintNotEnforced(
                constraint=constraint.type.value, adapter=DatabricksAdapter.type()
            )
        )


def warn_if_unsupported_for_columns(constraint: ColumnLevelConstraint) -> None:
    if constraint.warn_unsupported and constraint.type not in SUPPORTED_FOR_COLUMN:
        logger.warning(
            f"While constraint of type {constraint.type} is supported for models,"
            " it is not supported for columns."
        )
        warn_or_error(
            ConstraintNotSupported(
                constraint=constraint.type.value, adapter=DatabricksAdapter.type()
            )
        )


def warn_if_unsupported_for_models(constraint: ModelLevelConstraint) -> None:
    if constraint.warn_unsupported and constraint.type == ConstraintType.not_null:
        logger.warning(
            f"While constraint of type {constraint.type} is supported for columns,"
            " it is not supported for models."
        )
        warn_or_error(
            ConstraintNotSupported(
                constraint=constraint.type.value, adapter=DatabricksAdapter.type()
            )
        )


def process_column_constraint(constraint: ColumnLevelConstraint) -> Optional[str]:
    if constraint.type == ConstraintType.custom:
        return render_column_constraint(constraint)

    warn_if_unsupported(constraint)
    warn_if_unsupported_for_columns(constraint)
    warn_if_unenforced(constraint)

    if constraint in SUPPORTED_FOR_COLUMN:
        return render_column_constraint(constraint)

    return None


def render_column_constraint(constraint: ColumnLevelConstraint) -> Optional[str]:
    rendered_constraint = ""
    if constraint.name:
        rendered_constraint = f"CONSTRAINT {constraint.name} "
    if constraint.type == ConstraintType.primary_key:
        rendered_constraint += render_primary_key_for_column(constraint)
    elif constraint.type == ConstraintType.foreign_key:
        rendered_constraint += render_foreign_key_for_column(constraint)
    elif constraint.type == ConstraintType.custom:
        rendered_constraint += render_custom(constraint)

    if rendered_constraint:
        rendered_constraint = rendered_constraint.strip()

    return rendered_constraint if rendered_constraint != "" else None


def render_primary_key_for_column(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.primary_key
    rendered = "PRIMARY KEY"
    if constraint.expression:
        rendered += f" ({constraint.expression})"
    return rendered


def render_foreign_key_for_column(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.foreign_key
    rendered = "FOREIGN KEY REFERENCES"
    if constraint.expression:
        return rendered + f" ({constraint.expression})"
    elif constraint.to and constraint.to_columns:
        return rendered + f" {constraint.to} ({', '.join(constraint.to_columns)})"
    raise DbtValidationError(
        f"Foreign key constraint {constraint.name} is missing required fields:"
        " either 'expression' or 'to' and 'to_columns'"
    )


def process_model_constraint(constraint: ModelLevelConstraint) -> Optional[str]:
    if constraint.type == ConstraintType.custom:
        return render_model_constraint(constraint)

    warn_if_unsupported(constraint)
    warn_if_unsupported_for_models(constraint)
    warn_if_unenforced(constraint)

    if supported(constraint):
        return render_model_constraint(constraint)

    return None


def render_model_constraint(constraint: ModelLevelConstraint) -> Optional[str]:
    rendered_constraint = ""

    if constraint.name:
        rendered_constraint = f"CONSTRAINT {constraint.name} "
    if constraint.type == ConstraintType.check:
        return render_check(constraint)
    if constraint.type == ConstraintType.primary_key:
        rendered_constraint += render_primary_key_for_model(constraint)
    elif constraint.type == ConstraintType.foreign_key:
        rendered_constraint += render_foreign_key_for_model(constraint)
    elif constraint.type == ConstraintType.custom:
        rendered_constraint += render_custom(constraint)

    if rendered_constraint:
        rendered_constraint = rendered_constraint.strip()

    return rendered_constraint if rendered_constraint != "" else None


def render_primary_key_for_model(constraint: ModelLevelConstraint) -> str:
    prefix = render_primary_key_for_column(constraint)
    if constraint.expression:
        return prefix
    if constraint.columns:
        return f"{prefix} ({', '.join(constraint.columns)})"
    raise DbtValidationError(
        f"Primary key constraint {constraint.name} is missing required field:"
        " either 'columns' or 'expression' must be provided"
    )


def render_foreign_key_for_model(constraint: ModelLevelConstraint) -> str:
    assert constraint.type == ConstraintType.foreign_key
    rendered = "FOREIGN KEY"
    if constraint.to and constraint.to_columns:
        columns = ", ".join(constraint.columns)
        to_columns = ", ".join(constraint.to_columns)
        return rendered + f"({columns}) references {constraint.to} ({to_columns})"
    elif constraint.expression:
        return rendered + constraint.expression

    raise DbtValidationError(
        f"Foreign key constraint {constraint.name} is missing required fields:"
        " either 'expression' or 'columns', 'to', and 'to_columns' must be provided"
    )


def render_custom(constraint: ColumnLevelConstraint) -> str:
    assert constraint.type == ConstraintType.custom
    if constraint.expression:
        return constraint.expression
    raise DbtValidationError(
        f"Custom constraint {constraint.name} is missing required field: 'expression'"
    )


def render_check(constraint: ColumnLevelConstraint) -> Optional[str]:
    assert constraint.type == ConstraintType.check
    if not constraint.expression:
        return None
    return f"CHECK ({constraint.expression})"


def supported(constraint: ColumnLevelConstraint) -> bool:
    return DatabricksAdapter.CONSTRAINT_SUPPORT[constraint.type] != ConstraintSupport.NOT_SUPPORTED


def enforced(constraint: ColumnLevelConstraint) -> bool:
    return DatabricksAdapter.CONSTRAINT_SUPPORT[constraint.type] not in [
        ConstraintSupport.NOT_ENFORCED,
        ConstraintSupport.NOT_SUPPORTED,
    ]


def parse_column_constraint(raw_constraint: dict[str, Any]) -> ColumnLevelConstraint:
    try:
        ColumnLevelConstraint.validate(raw_constraint)
        return ColumnLevelConstraint.from_dict(raw_constraint)
    except Exception:
        raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")


def parse_model_constraint(raw_constraint: dict[str, Any]) -> ModelLevelConstraint:
    try:
        ModelLevelConstraint.validate(raw_constraint)
        return ModelLevelConstraint.from_dict(raw_constraint)
    except Exception:
        raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")
