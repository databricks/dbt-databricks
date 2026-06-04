from unittest.mock import Mock

import pytest

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.relation_configs.liquid_clustering import (
    LiquidClusteringConfig,
    LiquidClusteringProcessor,
)


@pytest.fixture(autouse=True)
def reset_managed_iceberg():
    original = GlobalState.get_use_managed_iceberg()
    yield
    GlobalState.set_use_managed_iceberg(original)


class TestLiquidClusteringProcessor:
    def test_from_model_node__none(self):
        model = Mock()
        model.config.extra = {}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[], auto_cluster=False)

    def test_from_model_node__liquid_clustered_by_string(self):
        model = Mock()
        model.config.extra = {"liquid_clustered_by": "id"}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["id"], auto_cluster=False)

    def test_from_model_node__liquid_clustered_by_list(self):
        model = Mock()
        model.config.extra = {"liquid_clustered_by": ["a", "b"]}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["a", "b"], auto_cluster=False)

    def test_from_model_node__auto_cluster(self):
        model = Mock()
        model.config.extra = {"auto_liquid_cluster": True}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[], auto_cluster=True)

    def test_from_model_node__managed_iceberg_partition_by_treated_as_cluster(self):
        # On managed Iceberg, partition_by is stored as liquid clustering server-side,
        # so the desired clustering must include partition_by to avoid a phantom wipe.
        GlobalState.set_use_managed_iceberg(True)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "partition_by": ["business_date"],
        }
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["business_date"], auto_cluster=False)

    def test_from_model_node__managed_iceberg_partition_by_string(self):
        GlobalState.set_use_managed_iceberg(True)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "partition_by": "business_date",
        }
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["business_date"], auto_cluster=False)

    def test_from_model_node__delta_partition_by_not_treated_as_cluster(self):
        # Non-iceberg: partition_by is real partitioning, not clustering.
        GlobalState.set_use_managed_iceberg(True)
        model = Mock()
        model.config.extra = {"partition_by": ["business_date"]}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[], auto_cluster=False)

    def test_from_model_node__uniform_iceberg_partition_by_not_treated_as_cluster(self):
        # UniForm (use_managed_iceberg=False) is Delta-backed; partition_by stays partitioning.
        GlobalState.set_use_managed_iceberg(False)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "partition_by": ["business_date"],
        }
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[], auto_cluster=False)

    def test_from_model_node__liquid_clustered_by_wins_over_partition_by(self):
        GlobalState.set_use_managed_iceberg(True)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "liquid_clustered_by": ["id"],
            "partition_by": ["business_date"],
        }
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["id"], auto_cluster=False)
