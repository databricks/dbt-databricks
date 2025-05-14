from agate import Table

from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.constraints import ConstraintsConfig
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
            "describe_extended": Table(
                rows=[
                    ["column", "string", "test comment"],
                ],
                column_names=["col_name", "col_type", "comment"],
            ),
            # TODO: Update this part
        }

        config = IncrementalTableConfig.from_results(results)

        assert config == IncrementalTableConfig(
            config={
                "comment": CommentConfig(comment=None, persist=False),
                "tags": TagsConfig(set_tags={"tag1": "value1", "tag2": "value2"}),
                "column_comments": ColumnCommentsConfig(
                    comments={"column": "test comment"}, quoted={}, persist=False
                ),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "f1"}),
                "liquid_clustering": LiquidClusteringConfig(
                    auto_cluster=True,
                    cluster_by=["col1", '"a"'],
                ),
                "constraints": ConstraintsConfig(
                    model_config={"frozen": True},
                    set_non_nulls=[],
                    unset_non_nulls=[],
                    set_constraints=[],
                    unset_constraints=[],
                ),
            }
        )
