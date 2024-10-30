from abc import ABC, abstractmethod
from typing import Any, Optional, Union
from dbt_common.contracts.config.base import BaseConfig
from dbt_common.exceptions import CompilationError
from dbt_common.contracts.constraints import ColumnLevelConstraint


class ConstraintsBehavior(ABC):
    @classmethod
    @abstractmethod
    def validate_constraints(cls, config: BaseConfig, is_view: bool, is_incremental: bool) -> bool:
        pass

    @classmethod
    def _inner_validate_constraints(
        cls, config: BaseConfig, is_view: bool, is_incremental: bool
    ) -> None:
        file_format = config.get("file_format", "delta")
        if file_format != "delta":
            raise CompilationError(f"Constraints are not supported for file format: {file_format}")
        if is_view:
            raise CompilationError("Constraints are not supported for views.")
        if is_incremental:
            raise CompilationError(
                "Constraints are not applied for incremental updates. "
                "Full refresh is required to update constraints."
            )


class DbtConstraintsBehavior(ConstraintsBehavior):
    @classmethod
    def validate_constraints(cls, config: BaseConfig, is_view: bool, is_incremental: bool) -> bool:
        has_model_contract = config.get("contract", {}).get("enforced", False)
        if has_model_contract:
            cls._inner_validate_constraints(config, is_view, is_incremental)
            return True
        return False


class DatabricksConstraintsBehavior(ConstraintsBehavior):
    @classmethod
    def validate_constraints(cls, config: BaseConfig, is_view: bool, is_incremental: bool) -> bool:
        has_model_contract = config.get("contract", {}).get("enforced", False)
        has_databricks_constraints = config.get("persist_constraints", False)
        if has_model_contract or has_databricks_constraints:
            cls._inner_validate_constraints(config, is_view, is_incremental)
            return True
        return False

    @classmethod
    def convert_databricks_model_constraints_to_dbt(cls, constraints: list[dict[str, Any]]]) -> list[dict[str, Any]]:
        dbt_constraints = []
        for constraint in constraints:
            if constraint.get("type"):
                # Looks like a dbt constraint
                dbt_constraints.append(constraint)
            elif not constraint.get("name"):
                raise CompilationError("Invalid check constraint name")
            elif not constraint.get("condition"):
                raise CompilationError("Invalid check constraint condition")
            else:
                dbt_constraints.append(
                    {
                        "name": constraint["name"],
                        "type": "check",
                        "expression": constraint["condition"],
                    }
                )
        return dbt_constraints

    @classmethod
    def convert_databricks_column_constraints_to_dbt(
        cls, constraints: list[Union[dict[str, Any], str]], column: dict[str, Any]
    ) -> list[dict[str, Any]]:
        dbt_constraints = []
        for constraint in constraints:
            if isinstance(constraint, dict) and constraint.get("type"):
                # Looks like a dbt constraint
                dbt_constraints.append(constraint)
            elif constraint == "not_null":
                dbt_constraints.append({"type": "not_null", "columns": [column.get("name")]})
            else:
                raise CompilationError(
                    f"Invalid constraint for column {column.get('name', '')}."
                    " Only `not_null` is supported."
                )
        return dbt_constraints

    @classmethod
    def _inner_validate_column_constraints(cls, constraint: dict[str, Any]) -> None:
