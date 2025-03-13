from unittest.mock import Mock

import pytest
from agate import Row

from dbt.adapters.databricks.relation_configs.query import QueryConfig, QueryProcessor
from dbt.exceptions import DbtRuntimeError

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
