from unittest.mock import Mock

import pytest
from agate import Row
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.query import (
    DescribeQueryProcessor,
    QueryConfig,
    QueryProcessor,
)

sql = "select * from foo"


class TestQueryProcessor:
    def test_from_results(self):
        results = {"information_schema.views": Row([sql, "other"], ["view_definition", "comment"])}
        spec = QueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query=sql)

    def test_from_model_node__with_query(self):
        model = Mock()
        model.compiled_code = sql
        spec = QueryProcessor.from_relation_config(model)
        assert spec == QueryConfig(query=sql)

    def test_from_model_node__without_query(self):
        model = Mock()
        model.compiled_code = None
        model.identifier = "1"
        with pytest.raises(
            DbtRuntimeError,
            match="Cannot compile model 1 with no SQL query",
        ):
            _ = QueryProcessor.from_relation_config(model)

    def test_from_relation_results__empty_view_definition(self):
        results = {"information_schema.views": Row(["", "other"], ["view_definition", "comment"])}
        spec = QueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query="")

    def test_get_diff__similar_query(self):
        model = QueryConfig(query="select * from foo")
        results = {
            "information_schema.views": Row(
                ["(\nselect * from foo\n)", "other"], ["view_definition", "comment"]
            )
        }
        other = QueryProcessor.from_relation_results(results)
        assert model.get_diff(other) is None

    def test_get_diff__different_query(self):
        model = QueryConfig(query="select * from foo")
        results = {
            "information_schema.views": Row(
                ["(\nselect * from bar\n)", "other"], ["view_definition", "comment"]
            )
        }
        other = QueryProcessor.from_relation_results(results)
        assert model.get_diff(other) is model

    def test_from_relation_results__none_view_definition(self):
        results = {"information_schema.views": Row([None, ""], ["view_definition", "comment"])}
        spec = QueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query="")

    def test_from_relation_results__missing_view_definition(self):
        empty_row = Row(values=set())
        results = {"information_schema.views": empty_row}
        spec = QueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query="")

    def test_get_diff__empty_existing_query(self):
        existing = QueryConfig(query="")
        new = QueryConfig(query="select 1")
        assert new.get_diff(existing) is new


class TestDescribeQueryProcessor:
    def test_from_relation_results__missing_view_text_row(self):
        results = {
            "describe_extended": [
                ("col_name", "data_type", "comment"),
                ("id", "int", ""),
            ]
        }
        spec = DescribeQueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query="")

    def test_from_relation_results__malformed_view_text_row(self):
        results = {"describe_extended": [("View Text",)]}
        with pytest.raises(
            DbtRuntimeError,
            match="Unexpected result from DESCRIBE EXTENDED: missing View Text value",
        ):
            DescribeQueryProcessor.from_relation_results(results)

    def test_from_relation_results__valid_view_text_row(self):
        sql = "select 1 as id"
        results = {
            "describe_extended": [
                ("col_name", "data_type", "comment"),
                ("View Text", sql, ""),
            ]
        }
        spec = DescribeQueryProcessor.from_relation_results(results)
        assert spec == QueryConfig(query=sql)
