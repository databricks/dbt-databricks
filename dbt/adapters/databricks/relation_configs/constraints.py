from dataclasses import asdict
from typing import ClassVar, Optional, cast

from agate import Table

from dbt.adapters.contracts.relation import RelationConfig
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
from dbt.adapters.relation_configs.config_base import RelationResults


class ConstraintsConfig(DatabricksComponentConfig):
    """Component encapsulating the constraints of a relation."""

    set_non_nulls: list[str]
    unset_non_nulls: list[str] = []
    set_constraints: list[TypedConstraint]
    unset_constraints: list[TypedConstraint] = []

    class Config:
        arbitrary_types_allowed = True

    def get_diff(self, other: "ConstraintsConfig") -> Optional["ConstraintsConfig"]:
        constraints_to_unset = []
        non_nulls_to_unset = []
        # Find constraints that need to be unset
        for constraint in other.set_constraints:
            if constraint not in self.set_constraints:
                constraints_to_unset.append(constraint)
        # Find non-nulls that need to be unset
        for non_null in other.set_non_nulls:
            if non_null not in self.set_non_nulls:
                non_nulls_to_unset.append(non_null)

        # Set constraints that exist in self but not in other
        set_constraints = [c for c in self.set_constraints if c not in other.set_constraints]
        set_non_nulls = [n for n in self.set_non_nulls if n not in other.set_non_nulls]

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
    ) -> list[CheckConstraint]:
        check_constraints = []
        if check_constraints_table:
            for row in check_constraints_table.rows:
                if row[0].startswith("delta.constraints."):
                    check_constraints.append(
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
    ) -> list[PrimaryKeyConstraint]:
        pk_constraints = []
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
                pk_constraints.append(
                    PrimaryKeyConstraint(
                        type=ConstraintType.primary_key, name=constraint_name, columns=columns
                    )
                )
        return pk_constraints

    @classmethod
    def _process_foreign_key_constraints(
        cls, fk_constraints_table: Optional[Table]
    ) -> list[ForeignKeyConstraint]:
        fk_constraints = []
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
                fk_constraints.append(
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
            [row[0] for row in non_null_constraint_columns.rows]
            if non_null_constraint_columns
            else []
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
            set_constraints=cast(list[TypedConstraint], all_constraints),
        )

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ConstraintsConfig:
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
            set_non_nulls=list(non_nulls),
            set_constraints=other_constraints,
        )
