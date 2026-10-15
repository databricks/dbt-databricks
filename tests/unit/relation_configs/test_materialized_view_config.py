from unittest.mock import MagicMock, Mock

from agate import Row, Table

from dbt.adapters.databricks.impl import MaterializedViewAPI
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.query import QueryConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterConfig
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
                rows=[["prop", "1"], ["other", "other"]], column_names=["key", "value"]
            ),
            "information_schema.tags": Table(
                rows=[["a", "b"], ["c", "d"]], column_names=["tag_name", "tag_value"]
            ),
            "information_schema.column_tags": Table(
                rows=[["col_a", "classification", "internal"]],
                column_names=["column_name", "tag_name", "tag_value"],
            ),
        }

        config = MaterializedViewConfig.from_results(results)

        assert config == MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "row_filter": RowFilterConfig(),
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
        model.columns = {"col_a": {"_extra": {"databricks_tags": {"classification": "internal"}}}}

        config = MaterializedViewConfig.from_relation_config(model)

        assert config == MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment", persist=True),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select * from foo"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "row_filter": RowFilterConfig(),
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
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "row_filter": RowFilterConfig(),
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
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "row_filter": RowFilterConfig(),
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
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "row_filter": RowFilterConfig(),
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
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "row_filter": RowFilterConfig(),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset.has_changes
        assert changeset.requires_full_refresh
        assert changeset.changes == {
            "partition_by": PartitionedByConfig(partition_by=["col_a"]),
            "refresh": RefreshConfig(cron="*/5 * * * *"),
            "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            "column_tags": ColumnTagsConfig(
                set_column_tags={"col_a": {"classification": "internal"}}
            ),
        }

    def test_get_changeset__tags_include_only_changed_keys(self):
        old = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=[]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(),
                "tblproperties": TblPropertiesConfig(tblproperties={}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select 1 as id"),
                "tags": TagsConfig(set_tags={"unchanged": "value", "updated": "old"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "row_filter": RowFilterConfig(),
            }
        )
        new = MaterializedViewConfig(
            config={
                **old.config,
                "tags": TagsConfig(
                    set_tags={"unchanged": "value", "updated": "new", "added": "value"}
                ),
            }
        )

        changeset = new.get_changeset(old)

        assert changeset is not None
        assert not changeset.requires_full_refresh
        assert changeset.changes == {
            "tags": TagsConfig(set_tags={"updated": "new", "added": "value"})
        }

    def test_get_changeset__column_tags_include_only_changed_keys(self):
        old = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=[]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(),
                "tblproperties": TblPropertiesConfig(tblproperties={}),
                "refresh": RefreshConfig(),
                "query": QueryConfig(query="select 1 as id"),
                "tags": TagsConfig(set_tags={}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={
                        "id": {"classification": "internal", "owner": "analytics"},
                        "unchanged": {"owner": "analytics"},
                    }
                ),
                "row_filter": RowFilterConfig(),
            }
        )
        new = MaterializedViewConfig(
            config={
                **old.config,
                "column_tags": ColumnTagsConfig(
                    set_column_tags={
                        "id": {"classification": "public", "owner": "analytics"},
                        "unchanged": {"owner": "analytics"},
                    }
                ),
            }
        )

        changeset = new.get_changeset(old)

        assert changeset is not None
        assert not changeset.requires_full_refresh
        assert changeset.changes == {
            "column_tags": ColumnTagsConfig(set_column_tags={"id": {"classification": "public"}})
        }


class TestMaterializedViewAPIDescribeRelation:
    def test_describe_relation_fetches_tags(self):
        # Without fetching tags here, an MV with `databricks_tags` would show a
        # spurious tag diff on every run, routing to ALTER instead of REFRESH.
        adapter = MagicMock()
        adapter.execute_macro.return_value = MagicMock()

        results = MaterializedViewAPI._describe_relation(adapter, MagicMock())

        assert "information_schema.tags" in results
        macro_names = {call.args[0] for call in adapter.execute_macro.call_args_list}
        assert "fetch_tags" in macro_names
