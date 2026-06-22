from dataclasses import asdict
from typing import ClassVar, Optional

import sqlparse  # type: ignore[import-untyped]
from agate import Table
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults

from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    ConstraintType,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    TypedConstraint,
    parse_constraints,
)
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class ConstraintsConfig(DatabricksComponentConfig):
    """Component encapsulating the constraints of a relation."""

    set_non_nulls: set[str]
    unset_non_nulls: set[str] = set()
    set_constraints: set[TypedConstraint]
    unset_constraints: set[TypedConstraint] = set()

    def normalize_expression(self, expression: Optional[str]) -> str:
        if expression:
            return sqlparse.format(
                expression, reindent=True, keyword_case="lower", identifier_case="lower"
            )
        else:
            return ""

    def normalize_constraint(self, constraint: TypedConstraint) -> TypedConstraint:
        """
        Normalize a constraint for comparison by standardizing format
        and removing fields that Databricks does not round-trip via information_schema.
        This is necessary because Databricks :
        - Reformats expressions for check constraints
        - Does not persist the `columns` in check constraints
        - Does not expose PK/FK RELY/NORELY in information_schema (#1513)
        - Does not persist FK `to`/`to_columns` on expression-form FKs
        """
        if isinstance(constraint, CheckConstraint):
            return CheckConstraint(
                type=constraint.type,
                name=constraint.name,
                expression=self.normalize_expression(constraint.expression),
                warn_unenforced=constraint.warn_unenforced,
                warn_unsupported=constraint.warn_unsupported,
                to=constraint.to,
                to_columns=constraint.to_columns,
                columns=[],
            )
        elif isinstance(constraint, PrimaryKeyConstraint):
            return PrimaryKeyConstraint(
                type=constraint.type,
                name=constraint.name,
                columns=constraint.columns,
            )
        elif isinstance(constraint, ForeignKeyConstraint):
            return ForeignKeyConstraint(
                type=constraint.type,
                name=constraint.name,
                columns=constraint.columns,
            )
        else:
            return constraint

    def get_diff(self, other: "ConstraintsConfig") -> Optional["ConstraintsConfig"]:
        # Diff on normalized keys; emit original constraints for ADD/DROP SQL.
        self_by_key = {self.normalize_constraint(c): c for c in self.set_constraints}
        other_by_key = {self.normalize_constraint(c): c for c in other.set_constraints}

        # Find constraints that need to be unset
        constraints_to_unset = {
            original for key, original in other_by_key.items() if key not in self_by_key
        }
        # Find non-nulls that need to be unset
        non_nulls_to_unset = other.set_non_nulls - self.set_non_nulls

        # Set constraints that exist in self but not in other
        set_constraints = {
            original for key, original in self_by_key.items() if key not in other_by_key
        }
        set_non_nulls = self.set_non_nulls - other.set_non_nulls

        # Reconcile same-name FKs whose target changed (`to`/`to_columns`).
        # Skip expression-form FKs: the target lives in `expression`, which the catalog
        # does not round-trip, so comparing `to`/`to_columns` would churn every run.
        for key in self_by_key.keys() & other_by_key.keys():
            model_constraint = self_by_key[key]
            catalog_constraint = other_by_key[key]
            if (
                isinstance(model_constraint, ForeignKeyConstraint)
                and model_constraint.to is not None
                and (model_constraint.to, tuple(model_constraint.to_columns or ()))
                != (catalog_constraint.to, tuple(catalog_constraint.to_columns or ()))
            ):
                set_constraints.add(model_constraint)
                constraints_to_unset.add(catalog_constraint)

        if set_constraints or set_non_nulls or constraints_to_unset or non_nulls_to_unset:
            return ConstraintsConfig(
                set_non_nulls=set_non_nulls,
                unset_non_nulls=non_nulls_to_unset,
                set_constraints=set_constraints,
                unset_constraints=constraints_to_unset,
            )
        return None


class ConstraintsProcessor(DatabricksComponentProcessor[ConstraintsConfig]):
    name: ClassVar[str] = "constraints"

    @classmethod
    def _process_check_constraints(
        cls, check_constraints_table: Optional[Table]
    ) -> set[CheckConstraint]:
        check_constraints = set()
        if check_constraints_table:
            for row in check_constraints_table.rows:
                if row[0].startswith("delta.constraints."):
                    check_constraints.add(
                        CheckConstraint(
                            type=ConstraintType.check,
                            name=row[0].replace("delta.constraints.", ""),
                            expression=row[1],
                        )
                    )
        return check_constraints

    @classmethod
    def _process_primary_key_constraints(
        cls, pk_constraints_table: Optional[Table]
    ) -> set[PrimaryKeyConstraint]:
        pk_constraints = set()
        if pk_constraints_table:
            # Group columns by constraint name
            constraint_columns: dict[str, list[str]] = {}
            for row in pk_constraints_table.rows:
                constraint_name = row[0]
                column_name = row[1]
                if constraint_name not in constraint_columns:
                    constraint_columns[constraint_name] = []
                constraint_columns[constraint_name].append(column_name)

            # Create one PrimaryKeyConstraint per unique constraint name
            for constraint_name, columns in constraint_columns.items():
                pk_constraints.add(
                    PrimaryKeyConstraint(
                        type=ConstraintType.primary_key, name=constraint_name, columns=columns
                    )
                )
        return pk_constraints

    @classmethod
    def _process_foreign_key_constraints(
        cls, fk_constraints_table: Optional[Table]
    ) -> set[ForeignKeyConstraint]:
        fk_constraints = set()
        if fk_constraints_table:
            # Group by constraint_name
            fk_data: dict[str, dict] = {}
            for row in fk_constraints_table.rows:
                constraint_name = row[0]
                if constraint_name not in fk_data:
                    fk_data[constraint_name] = {
                        "columns": [],
                        "to": f"`{row[2]}`.`{row[3]}`.`{row[4]}`",
                        "to_columns": [],
                    }
                fk_data[constraint_name]["columns"].append(row[1])
                fk_data[constraint_name]["to_columns"].append(row[5])

            # Create ForeignKeyConstraint objects
            for constraint_name, data in fk_data.items():
                fk_constraints.add(
                    ForeignKeyConstraint(
                        type=ConstraintType.foreign_key,
                        name=constraint_name,
                        columns=data["columns"],
                        to=data["to"],
                        to_columns=data["to_columns"],
                    )
                )
        return fk_constraints

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ConstraintsConfig:
        non_null_constraint_columns = results.get("non_null_constraint_columns")
        to_set_non_nulls = (
            {row[0] for row in non_null_constraint_columns.rows}
            if non_null_constraint_columns
            else set()
        )

        table_properties = results.get("show_tblproperties")
        primary_key_constraints = results.get("primary_key_constraints")
        foreign_key_constraints = results.get("foreign_key_constraints")

        check_constraints = cls._process_check_constraints(table_properties)
        pk_constraints = cls._process_primary_key_constraints(primary_key_constraints)
        fk_constraints = cls._process_foreign_key_constraints(foreign_key_constraints)

        all_constraints: list[TypedConstraint] = []
        all_constraints.extend(check_constraints)
        all_constraints.extend(pk_constraints)
        all_constraints.extend(fk_constraints)

        return ConstraintsConfig(
            set_non_nulls=to_set_non_nulls,
            set_constraints=set(all_constraints),
        )

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ConstraintsConfig:
        # Only read constraints from the model when contract enforcement is enabled.
        # Without this guard, constraints that dbt-core parses and stores regardless of
        # contract enforcement would leak through and be applied to the database.
        # See: https://github.com/databricks/dbt-databricks/issues/1342
        config = getattr(relation_config, "config", None)
        contract = getattr(config, "contract", None) if config else None
        contract_enforced = getattr(contract, "enforced", False)

        if not contract_enforced:
            return ConstraintsConfig(
                set_non_nulls=set(),
                set_constraints=set(),
            )

        constraints = getattr(relation_config, "constraints", [])
        constraints = [
            {
                "name": (item["name"] if isinstance(item, dict) else item.name),
                **(item if isinstance(item, dict) else asdict(item)),
            }
            for item in constraints
        ]

        columns = getattr(relation_config, "columns", {})
        columns = [
            {"name": name, **(col if isinstance(col, dict) else asdict(col))}
            for name, col in columns.items()
        ]

        non_nulls, other_constraints = parse_constraints(columns, constraints)

        return ConstraintsConfig(
            set_non_nulls=set(non_nulls),
            set_constraints=set(other_constraints),
        )
