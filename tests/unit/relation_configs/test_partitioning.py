from mock import Mock
import pytest
from agate import Table

from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByConfig,
    PartitionedByProcessor,
)


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
    def test_to_sql_clause(self, input, expected):
        config = PartitionedByConfig(input)
        assert config.to_sql_clause() == expected


class TestPartitionedByProcessor:
    def test_from_results__none(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    [None, None, None],
                    ["# Detailed Table Information", None, None],
                    ["Catalog:", "default", None],
                ]
            )
        }

        spec = PartitionedByProcessor.from_results(results)
        assert spec == PartitionedByConfig([])

    def test_from_results__single(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    ["# Partition Information", None, None],
                    ["# col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    [None, None, None],
                    ["# Detailed Table Information", None, None],
                    ["Catalog:", "default", None],
                ]
            )
        }

        spec = PartitionedByProcessor.from_results(results)
        assert spec == PartitionedByConfig(["col_a"])

    def test_from_results__multiple(self):
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
                ]
            )
        }
        spec = PartitionedByProcessor.from_results(results)
        assert spec == PartitionedByConfig(["col_a", "col_b"])

    def test_from_model_node__without_partition_by(self):
        model = Mock()
        model.config.extra = {}
        spec = PartitionedByProcessor.from_model_node(model)
        assert spec == PartitionedByConfig(None)

    def test_from_model_node__single_column(self):
        model = Mock()
        model.config.extra = {"partition_by": "col_a"}
        spec = PartitionedByProcessor.from_model_node(model)
        assert spec == PartitionedByConfig(["col_a"])

    def test_from_model_node__multiple_columns(self):
        model = Mock()
        model.config.extra = {"partition_by": ["col_a", "col_b"]}
        spec = PartitionedByProcessor.from_model_node(model)
        assert spec == PartitionedByConfig(["col_a", "col_b"])
