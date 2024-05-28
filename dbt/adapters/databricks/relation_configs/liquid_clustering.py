import json
from typing import Any
from typing import ClassVar
from typing import List

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentProcessor
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError


class LiquidClusteringConfig(DatabricksComponentConfig):
    """Component encapsulating the liquid clustering columns of a relation."""

    cluster_by: List[str]

    def __eq__(self, __value: Any) -> bool:
        """Override equality check to ignore certain tblproperties."""

        if not isinstance(__value, LiquidClusteringConfig):
            return False

        return sorted(self.cluster_by) == sorted(__value.cluster_by)


class LiquidClusteringProcessor(DatabricksComponentProcessor[LiquidClusteringConfig]):
    name: ClassVar[str] = "liquid_clustered_by"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> LiquidClusteringConfig:
        table = results.get("show_tblproperties")
        cluster_by = []

        if table:
            for row in table.rows:
                if str(row[0]) == "clusteringColumns":
                    cluster_by = (
                        row[1]
                        .replace("]", "")
                        .replace("[", "")
                        .replace("'", "")
                        .replace('"', "")
                        .split(", ")
                    )
                    break

        return LiquidClusteringConfig(cluster_by=cluster_by)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> LiquidClusteringConfig:
        liquid_clustered_by = base.get_config_value(relation_config, "liquid_clustered_by")
        if not liquid_clustered_by:
            return LiquidClusteringConfig(cluster_by=[])
        if isinstance(liquid_clustered_by, List):
            return LiquidClusteringConfig(cluster_by=liquid_clustered_by)
        if isinstance(liquid_clustered_by, str):
            return LiquidClusteringConfig(cluster_by=[liquid_clustered_by])
        else:
            raise DbtRuntimeError("liquid_clustered_by must be a string or list of strings")
