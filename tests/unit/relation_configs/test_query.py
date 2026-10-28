from unittest.mock import Mock

import pytest
from agate import Row
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsConfig
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.query import (
    DescribeQueryProcessor,
    QueryConfig,
    QueryProcessor,
)
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.adapters.databricks.relation_configs.view import ViewConfig

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


class TestViewQueryChanges:
    @staticmethod
    def view_config(query="select id from foo", **components):
        return ViewConfig(
            config={
                "tags": TagsConfig(set_tags={}),
                "tblproperties": TblPropertiesConfig(tblproperties={}),
                "query": QueryConfig(query=query),
                "comment": CommentConfig(),
                "column_comments": ColumnCommentsConfig(comments={"id": ""}),
                "column_tags": ColumnTagsConfig(set_column_tags={}),
                **components,
            }
        )

    @pytest.mark.parametrize(
        "query",
        [
            "select * from foo",
            "select foo.* from foo",
            "with src as (select * from foo) select id from src",
            "select * except (secret) from foo",
            "select count(*) from foo",
            "select id * 2 from foo",
            "select '*' as literal from foo",
            "select id from foo -- *",
        ],
    )
    def test_unchanged_query_containing_star_is_reapplied(self, query):
        config = self.view_config(query)

        changeset = config.get_changeset(self.view_config(query))

        assert changeset is not None
        assert changeset.changes == {"query": config.config["query"]}
        assert not changeset.requires_full_refresh

    def test_unchanged_explicit_query_has_no_changes(self):
        assert self.view_config().get_changeset(self.view_config()) is None

    def test_changed_explicit_query_is_reapplied(self):
        config = self.view_config("select msg from foo")

        changeset = config.get_changeset(self.view_config())

        assert changeset is not None
        assert changeset.changes == {"query": config.config["query"]}

    def test_column_comment_only_change_does_not_reapply_query(self):
        comments = ColumnCommentsConfig(comments={"id": "new comment"}, persist=True)
        config = self.view_config(column_comments=comments)

        changeset = config.get_changeset(self.view_config())

        assert changeset is not None
        assert changeset.changes == {
            "column_comments": ColumnCommentsConfig(comments={"`id`": "new comment"}, persist=True)
        }
        assert not changeset.requires_full_refresh

    def test_star_query_preserves_other_configuration_changes(self):
        properties = TblPropertiesConfig(tblproperties={"key": "value"})
        config = self.view_config("select * from foo", tblproperties=properties)

        changeset = config.get_changeset(self.view_config("select * from foo"))

        assert changeset is not None
        assert changeset.changes == {"query": config.config["query"], "tblproperties": properties}

    @pytest.mark.parametrize("query", ["select id from foo", "select * from foo"])
    def test_description_change_still_requires_replacement(self, query):
        config = self.view_config(query, comment=CommentConfig(comment="new comment", persist=True))

        changeset = config.get_changeset(self.view_config(query))

        assert changeset is not None
        assert changeset.requires_full_refresh
