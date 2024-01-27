from agate import Table
from mock import Mock
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.streaming_table import StreamingTableConfig
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig


class TestStreamingTableConfig:
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
                ]
            ),
            "show_tblproperties": Table(rows=[["prop", "1"], ["other", "other"]]),
        }

        config = StreamingTableConfig.from_results(results)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
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
        }
        model.description = "This is the table comment"

        config = StreamingTableConfig.from_model_node(model)

        assert config == StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
            }
        )

    def test_get_changeset__no_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
            }
        )

        changeset = new.get_changeset(old)
        assert not changeset.requires_full_refresh
        assert changeset.changes == {
            "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
            "comment": CommentConfig(comment="This is the table comment"),
            "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
        }

    def test_get_changeset__some_changes(self):
        old = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a", "col_b"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(),
            }
        )
        new = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["col_a"]),
                "comment": CommentConfig(comment="This is the table comment"),
                "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
                "refresh": RefreshConfig(cron="*/5 * * * *"),
            }
        )

        changeset = new.get_changeset(old)
        assert changeset.has_changes
        assert changeset.requires_full_refresh
        assert changeset.changes == {
            "partition_by": PartitionedByConfig(partition_by=["col_a"]),
            "comment": CommentConfig(comment="This is the table comment"),
            "tblproperties": TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"}),
            "refresh": RefreshConfig(cron="*/5 * * * *"),
        }
