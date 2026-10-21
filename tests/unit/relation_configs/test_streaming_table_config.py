from unittest.mock import Mock

from agate import Table

from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.query import QueryConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterConfig
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from tests.unit import fixtures


class TestStreamingTableConfig:
    def test_from_results(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                partition_info=[
                    ["col_a", "int", "This is a comment"],
                    ["col_b", "int", "This is a comment"],
                ],
                detailed_table_info=[
                    ["Catalog:", "default", None],
                    ["Comment", "This is the table comment", None],
                    ["Refresh Schedule", "MANUAL", None],
                    ["View Text", "select * from foo", None],
                ],
            ),
            "show_tblproperties": fixtures.gen_tblproperties([["prop", "1"], ["other", "other"]]),
            "information_schema.tags": Table(
                rows=[["a", "b"], ["c", "d"]], column_names=["tag_name", "tag_value"]
            ),
            "information_schema.column_tags": Table(
                rows=[["col_a", "classification", "internal"]],
                column_names=["column_name", "tag_name", "tag_value"],
            ),
        }

        config = StreamingTableConfig.from_results(results)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "query": QueryConfig(query="select * from foo"),
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
        model.config.persist_docs = {"relation": False, "columns": True}
        model.description = "This is the table comment"
        model.columns = {"col_a": {"_extra": {"databricks_tags": {"classification": "internal"}}}}

        config = StreamingTableConfig.from_relation_config(model)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment", persist=False),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )

    def test_get_changeset__no_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )

        changeset = new.get_changeset(old)
        # Based on the new logic, when there are no changes, get_changeset returns None
        assert changeset is None

    def test_get_changeset__tblproperties_retains_already_applied(self):
        # A new tblproperty is added on top of one already applied to the relation. The
        # streaming-table alter path renders the changeset's tblproperties via
        # CREATE OR REFRESH (get_create_st_internal), so the changeset must carry the full
        # desired set, not just the newly added property.
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset is not None
        assert changeset.changes["tblproperties"] == TblPropertiesConfig(
            tblproperties={"prop": "1", "other": "other"}
        )
        assert "tags" not in changeset.changes
        assert "column_tags" not in changeset.changes

    def test_get_changeset__some_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a"]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(cron="*/5 * * * *"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"col_a": {"classification": "internal"}}
                ),
                "query": QueryConfig(query="select * from foo"),
                "row_filter": RowFilterConfig(),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset is not None
        assert changeset.has_changes
        assert changeset.requires_full_refresh
        assert changeset.changes == {
            "partition_by": PartitionedByConfig(partition_by=["col_a"]),
            "liquid_clustering": LiquidClusteringConfig(),
            "comment": CommentConfig(comment="This is the table comment"),
            "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
            "refresh": RefreshConfig(cron="*/5 * * * *"),
            "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            "column_tags": ColumnTagsConfig(
                set_column_tags={"col_a": {"classification": "internal"}}
            ),
            "query": QueryConfig(query="select * from foo"),
            "row_filter": RowFilterConfig(),
        }

    def test_get_changeset__tags_include_only_changed_keys(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=[]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(),
                "tblproperties": TblPropertiesConfig(tblproperties={}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"unchanged": "value", "updated": "old"}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                "query": QueryConfig(query="select stream(1 as id)"),
                "row_filter": RowFilterConfig(),
            }
        )
        new = StreamingTableConfig(
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
        assert changeset.changes["tags"] == TagsConfig(
            set_tags={"updated": "new", "added": "value"}
        )
        assert "column_tags" not in changeset.changes

    def test_get_changeset__column_tags_include_only_changed_keys(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=[]),
                "liquid_clustering": LiquidClusteringConfig(),
                "comment": CommentConfig(),
                "tblproperties": TblPropertiesConfig(tblproperties={}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={
                        "id": {"classification": "internal", "owner": "analytics"},
                        "unchanged": {"owner": "analytics"},
                    }
                ),
                "query": QueryConfig(query="select stream(1 as id)"),
                "row_filter": RowFilterConfig(),
            }
        )
        new = StreamingTableConfig(
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
        assert changeset.changes["column_tags"] == ColumnTagsConfig(
            set_column_tags={"id": {"classification": "public"}}
        )
        assert "tags" not in changeset.changes
