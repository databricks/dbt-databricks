from agate import Table

from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig


class TestIncrementalConfig:
    def test_from_results(self):
        results = {
            "information_schema.tags": Table(
                rows=[
                    ["tag1", "value1"],
                    ["tag2", "value2"],
                ],
                column_names=["tag_name", "tag_value"],
            ),
            "show_tblproperties": Table(
                rows=[
                    ["prop", "f1"],
                    ["clusterByAuto", "true"],
                    ["clusteringColumns", '[["col1"],[""a""]]'],
                ],
                column_names=["key", "value"],
            ),
        }

        config = IncrementalTableConfig.from_results(results)

        assert config == IncrementalTableConfig(
            config={
                "tags": TagsConfig(set_tags={"tag1": "value1", "tag2": "value2"}),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "f1"}),
                "liquid_clustering": LiquidClusteringConfig(
                    auto_cluster=True,
                    cluster_by=["col1", '"a"'],
                ),
            }
        )
