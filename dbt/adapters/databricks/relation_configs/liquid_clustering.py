from typing import ClassVar, Union

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


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
        cluster_by_auto: bool = bool(
            base.get_config_value(relation_config, "auto_liquid_cluster") or False
        )

        cluster_by = cls._as_list(base.get_config_value(relation_config, "liquid_clustered_by"))

        # Managed Iceberg stores `partition_by` as liquid clustering keys server-side, so the
        # existing relation reports them as clusteringColumns. Treat partition_by as the desired
        # clustering here, otherwise the reconciler sees a phantom removal and wipes it with
        # `CLUSTER BY NONE`. See https://github.com/databricks/dbt-databricks/issues/1495.
        if not cluster_by and cls._is_managed_iceberg(relation_config):
            cluster_by = cls._as_list(base.get_config_value(relation_config, "partition_by"))

        return LiquidClusteringConfig(cluster_by=cluster_by, auto_cluster=cluster_by_auto)

    @staticmethod
    def _as_list(value: Union[str, list[str], None]) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @staticmethod
    def _is_managed_iceberg(relation_config: RelationConfig) -> bool:
        table_format = base.get_config_value(relation_config, "table_format")
        return table_format == constants.ICEBERG_TABLE_FORMAT and bool(
            GlobalState.get_use_managed_iceberg()
        )

    @staticmethod
    def extract_cluster_by(cluster_by: str) -> list[str]:
        if not cluster_by or cluster_by == "[]":
            return []
        trimmed = cluster_by[1:-1]
        parts = trimmed.split(",")
        return [part[2:-2] for part in parts]
