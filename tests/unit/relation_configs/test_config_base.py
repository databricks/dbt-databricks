from dataclasses import dataclass
from agate import Row
from mock import Mock
import pytest

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfigBase,
    PartitionedByConfig,
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.comment import CommentConfig, CommentProcessor


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class TestDatabricksRelationConfig(DatabricksRelationConfigBase):
    partitioned_by_processor = PartitionedByProcessor
    config_components = [CommentProcessor]
    comment: CommentConfig
    partition_by: PartitionedByConfig


class TestFromDescribeExtended:
    def test_from_describe_extended__simple_case(self):
        results = [
            Row(["col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row([None, None, None]),
            Row(["# Detailed Table Information", None, None]),
            Row(["Catalog:", "default", None]),
            Row(["Schema:", "default", None]),
            Row(["Table:", "table_abc", None]),
            Row(["Comment", "This is the table comment", None]),
        ]
        config = TestDatabricksRelationConfig.from_describe_extended(results)
        assert config.comment == CommentConfig("This is the table comment")

    def test_from_describe_extended__with_partitions(self):
        results = [
            Row(["col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row(["# Partition Information", None, None]),
            Row(["# col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row([None, None, None]),
            Row(["# Detailed Table Information", None, None]),
            Row(["Catalog:", "default", None]),
            Row(["Schema:", "default", None]),
            Row(["Table:", "table_abc", None]),
            Row(["Comment", "This is the table comment", None]),
        ]
        config = TestDatabricksRelationConfig.from_describe_extended(results)
        assert config.comment == CommentConfig("This is the table comment")
        assert config.partition_by == PartitionedByConfig(["col_a"])


class TestPartitionedByConfig:
    @pytest.mark.parametrize(
        "input,expected",
        [
            (None, ""),
            ([], ""),
            (["col_a"], "PARTITIONED BY (col_a)"),
            (["col_a", "col_b"], "PARTITIONED BY (col_a, col_b)"),
        ],
    )
    def test_to_sql_clause__empty(self, input, expected):
        config = PartitionedByConfig(input)
        assert config.to_sql_clause() == expected


class TestPartitionedByProcessor:
    def test_process_partition_rows__none(self):
        results = [
            Row(["col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row([None, None, None]),
            Row(["# Detailed Table Information", None, None]),
            Row(["Catalog:", "default", None]),
        ]

        spec = PartitionedByProcessor.process_partition_rows(results)
        assert spec == PartitionedByConfig([])

    def test_process_partition_rows__single(self):
        results = [
            Row(["col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row(["# Partition Information", None, None]),
            Row(["# col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row([None, None, None]),
            Row(["# Detailed Table Information", None, None]),
            Row(["Catalog:", "default", None]),
        ]
        spec = PartitionedByProcessor.process_partition_rows(results)
        assert spec == PartitionedByConfig(["col_a"])

    def test_process_partition_rows__multiple(self):
        results = [
            Row(["col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row(["# Partition Information", None, None]),
            Row(["# col_name", "data_type", "comment"]),
            Row(["col_a", "int", "This is a comment"]),
            Row(["col_b", "int", "This is a comment"]),
            Row([None, None, None]),
            Row(["# Detailed Table Information", None, None]),
            Row(["Catalog:", "default", None]),
        ]
        spec = PartitionedByProcessor.process_partition_rows(results)
        assert spec == PartitionedByConfig(["col_a", "col_b"])

    def test_process_model_node__without_partition_by(self):
        model = Mock()
        model.config.extra.get.return_value = None
        spec = PartitionedByProcessor.process_model_node(model)
        assert spec == PartitionedByConfig(None)

    def test_process_model_node__single_column(self):
        model = Mock()
        model.config.extra.get.return_value = "col_a"
        spec = PartitionedByProcessor.process_model_node(model)
        assert spec == PartitionedByConfig(["col_a"])

    def test_process_model_node__multiple_columns(self):
        model = Mock()
        model.config.extra.get.return_value = ["col_a", "col_b"]
        spec = PartitionedByProcessor.process_model_node(model)
        assert spec == PartitionedByConfig(["col_a", "col_b"])
