from dataclasses import dataclass

from dbt.adapters.base.relation import Policy
from dbt.adapters.relation_configs import RelationConfigBase

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
