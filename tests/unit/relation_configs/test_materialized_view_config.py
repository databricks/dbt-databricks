from unittest.mock import Mock

from agate import Row, Table

from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.query import QueryConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig


class TestMaterializedViewConfig:
    def test_from_results(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    ["# Partition Information", None, None],
                    ["# col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    ["col_b", "int", "This is a comment"],
                    [None, None, None],
                    ["# Detailed Table Information", None, None],
                    ["Catalog:", "default", None],
                    ["Comment", "This is the table comment", None],
                    ["Refresh Schedule", "MANUAL", None],
                ],
                column_names=["col_name", "data_type", "comment"],
            ),
            "information_schema.views": Row(
                ["select * from foo", "other"], ["view_definition", "comment"]
            ),
            "show_tblproperties": Table(
                rows=[
                    ["prop", "1"],
                    ["other", "other"],
                    ["dbt.tblproperties.managedKeys", "other,prop"],
                ],
                column_names=["key", "value"],
            ),
            "information_schema.tags": Table(
                rows=[["a", "b"], ["c", "d"]], column_names=["tag_name", "tag_value"]
            ),
        }

        config = MaterializedViewConfig.from_results(results)

        assert config == MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={
                        "prop": "1",
                        "other": "other",
                        "dbt.tblproperties.managedKeys": "other,prop",
                    }
                ),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            }
        )

    def test_from_model_node(self):
        model = Mock()
        model.compiled_code = "select * from foo"
        model.config.extra = {
            "partition_by": ["col_a", "col_b"],
            "tblproperties": {
                "prop": "1",
                "other": "other",
            },
            "databricks_tags": {"a": "b", "c": "d"},
        }
        model.config.persist_docs = {"relation": True, "columns": False}
        model.description = "This is the table comment"

        config = MaterializedViewConfig.from_relation_config(model)

        assert config == MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment", persist=True),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={
                        "prop": "1",
                        "other": "other",
                        "dbt.tblproperties.managedKeys": "other,prop",
                    }
                ),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            }
        )

    def test_get_changeset__no_changes(self):
        old = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            }
        )
        new = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            }
        )

        assert new.get_changeset(old) is None

    def test_get_changeset__some_changes(self):
        old = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={}),
            }
        )
        new = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(cron="*/5 * * * *"),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset.has_changes
        assert changeset.requires_full_refresh
        assert changeset.changes == {
            "partition_by": PartitionedByConfig(partition_by=["col_a"]),
            "refresh": RefreshConfig(cron="*/5 * * * *"),
            "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
        }
