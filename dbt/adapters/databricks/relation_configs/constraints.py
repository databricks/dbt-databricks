from dataclasses import asdict
from typing import ClassVar, Optional

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.constraints import TypedConstraint, parse_constraints
from dbt.adapters.databricks.relation_configs import base
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
        for constraint in other.set_constraints:
            if constraint not in self.set_constraints:
                constraints_to_unset.append(constraint)
        for non_null in other.set_non_nulls:
            if non_null not in self.set_non_nulls:
                non_nulls_to_unset.append(non_null)
        if (
            self.set_constraints != other.set_constraints
            or self.set_non_nulls != other.set_non_nulls
            or constraints_to_unset
            or non_nulls_to_unset
        ):
            return ConstraintsConfig(
                set_non_nulls=self.set_non_nulls,
                unset_non_nulls=non_nulls_to_unset,
                set_constraints=self.set_constraints,
                unset_constraints=constraints_to_unset,
            )
        return None


class ConstraintsProcessor(DatabricksComponentProcessor[ConstraintsConfig]):
    name: ClassVar[str] = "constraints"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ConstraintsConfig:
        # TODO: Implement
        return ConstraintsConfig(set_non_nulls=[], set_constraints=[])

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ConstraintsConfig:
        constraints = [c.to_dict() for c in getattr(relation_config, "constraints", [])]

        columns = getattr(relation_config, "columns", {})
        columns = [
            {**asdict(col), "constraints": [c.to_dict() for c in col.constraints]}
            for col in columns.values()
        ]

        non_nulls, other_constraints = parse_constraints(columns, constraints)

        return ConstraintsConfig(
            set_non_nulls=list(non_nulls),
            set_constraints=other_constraints,
        )
