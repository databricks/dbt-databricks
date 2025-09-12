from unittest.mock import Mock

from agate import Table

from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.query import QueryConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
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
        }

        config = StreamingTableConfig.from_results(results)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "query": QueryConfig(query="select * from foo"),
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

        config = StreamingTableConfig.from_relation_config(model)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment", persist=False),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "query": QueryConfig(query="select * from foo"),
            }
        )

    def test_get_changeset__no_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "query": QueryConfig(query="select * from foo"),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "query": QueryConfig(query="select * from foo"),
            }
        )

        changeset = new.get_changeset(old)
        # Based on the new logic, when there are no changes, get_changeset returns None
        assert changeset is None

    def test_get_changeset__some_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
                "tags": TagsConfig(set_tags={}),
                "query": QueryConfig(query="select * from foo"),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(cron="*/5 * * * *"),
                "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
                "query": QueryConfig(query="select * from foo"),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset is not None
        assert changeset.has_changes
        assert changeset.requires_full_refresh
        assert changeset.changes == {
            "partition_by": PartitionedByConfig(partition_by=["col_a"]),
            "comment": CommentConfig(comment="This is the table comment"),
            "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
            "refresh": RefreshConfig(cron="*/5 * * * *"),
            "tags": TagsConfig(set_tags={"a": "b", "c": "d"}),
            "query": QueryConfig(query="select * from foo"),
        }
