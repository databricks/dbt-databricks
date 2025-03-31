from typing import ClassVar, Union

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class LiquidClusteringConfig(DatabricksComponentConfig):
    """Component encapsulating the liquid clustering options."""

    auto_cluster: bool = False
    cluster_by: list[str] = []


class LiquidClusteringProcessor(DatabricksComponentProcessor[LiquidClusteringConfig]):
    name: ClassVar[str] = "liquid_clustering"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> LiquidClusteringConfig:
        cluster_by_auto = False
        cluster_by: list[str] = []
        table = results["show_tblproperties"]
        for row in table.rows:
            if row[0] == "clusterByAuto":
                cluster_by_auto = row[1] == "true"
            if row[0] == "clusteringColumns":
                cluster_by = cls.extract_cluster_by(row[1])
        return LiquidClusteringConfig(cluster_by=cluster_by, auto_cluster=cluster_by_auto)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> LiquidClusteringConfig:
        liquid_clustered_by: Union[str, list[str], None] = base.get_config_value(
            relation_config, "liquid_clustered_by"
        )
        cluster_by_auto: bool = bool(
            base.get_config_value(relation_config, "auto_liquid_cluster") or False
        )
        if not liquid_clustered_by:
            return LiquidClusteringConfig(cluster_by=[], auto_cluster=cluster_by_auto)
        if isinstance(liquid_clustered_by, str):
            return LiquidClusteringConfig(
                cluster_by=[liquid_clustered_by], auto_cluster=cluster_by_auto
            )
        return LiquidClusteringConfig(cluster_by=liquid_clustered_by, auto_cluster=cluster_by_auto)

    @staticmethod
    def extract_cluster_by(cluster_by: str) -> list[str]:
        if not cluster_by or cluster_by == "[]":
            return []
        trimmed = cluster_by[1:-1]
        parts = trimmed.split(",")
        return [part[2:-2] for part in parts]
