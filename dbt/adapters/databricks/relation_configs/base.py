from dataclasses import dataclass
from typing import Optional

import agate
from dbt.adapters.base.relation import Policy
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.databricks.relation_configs.policies import (
    DatabricksIncludePolicy,
    DatabricksQuotePolicy,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationConfigBase(RelationConfigBase):
    """
    This base class implements a few boilerplate methods and provides some light structure for Databricks relations.
    """

    @classmethod
    def include_policy(cls) -> Policy:
        return DatabricksIncludePolicy()

    @classmethod
    def quote_policy(cls) -> Policy:
        return DatabricksQuotePolicy()

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> RelationConfigBase:
        relation_config = cls.parse_model_node(model_node)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        raise NotImplementedError(
            "`parse_model_node()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults) -> RelationConfigBase:
        relation_config = cls.parse_relation_results(relation_results)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        raise NotImplementedError(
            "`parse_relation_results()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def _render_part(cls, component: ComponentName, value: Optional[str]) -> Optional[str]:
        if cls.include_policy().get_part(component) and value:
            if cls.quote_policy().get_part(component):
                return f'"{value}"'
            return value.lower()
        return None

    @classmethod
    def _get_first_row(cls, results: agate.Table) -> agate.Row:
        try:
            return results.rows[0]
        except IndexError:
            return agate.Row(values=set())
