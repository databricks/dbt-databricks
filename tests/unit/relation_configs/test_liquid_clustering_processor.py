import pytest
from agate import Table
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import (
    LiquidClusteringProcessor,
)
from dbt.exceptions import DbtRuntimeError
from mock import Mock


class TestLiquidClusteringProcessor:
    def test_from_results__none(self):
        results = {"show_tblproperties": None}
        spec = LiquidClusteringProcessor.from_relation_results(results)
        assert spec == LiquidClusteringConfig(cluster_by=[])

    def test_from_results__single(self):
        results = {"show_tblproperties": Table(rows=[["clusteringColumns", [["f1"]]]])}
        spec = LiquidClusteringProcessor.from_relation_results(results)
        assert spec == LiquidClusteringConfig(cluster_by=["f1"])

    def test_from_results__multiple(self):
        results = {"show_tblproperties": Table(rows=[["clusteringColumns", [["f1"], ["f2"]]]])}
        spec = LiquidClusteringProcessor.from_relation_results(results)
        assert spec == LiquidClusteringConfig(cluster_by=["f1", "f2"])

    def test_from_model_node__without_liquid_clustered_by(self):
        model = Mock()
        model.config.extra = {}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[])

    def test_from_model_node__with_str_liquid_clustered_by(self):
        model = Mock()
        model.config.extra = {
            "liquid_clustered_by": "f1",
        }
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["f1"])

    def test_from_model_node__with_array_liquid_clustered_by(self):
        model = Mock()
        model.config.extra = {"liquid_clustered_by": ["f1", "f2"]}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=["f1", "f2"])

    def test_from_model_node__with_empty_liquid_clustered_by(self):
        model = Mock()
        model.config.extra = {"liquid_clustered_by": []}
        spec = LiquidClusteringProcessor.from_relation_config(model)
        assert spec == LiquidClusteringConfig(cluster_by=[])

    def test_from_model_node__with_incorrect_liquid_clustered_by(self):
        model = Mock()
        model.config.extra = {"liquid_clustered_by": 1}
        with pytest.raises(
            DbtRuntimeError,
            match="liquid_clustered_by must be a string or list of strings",
        ):
            _ = LiquidClusteringProcessor.from_relation_config(model)
