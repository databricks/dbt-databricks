import pytest
from agate import Table, Row
from dbt.adapters.databricks.relation_configs.comment import CommentConfig, CommentProcessor


class TestCommentConfig:
    @pytest.mark.parametrize(
        "input,expected", [(None, ""), ("", ""), ("comment", "COMMENT 'comment'")]
    )
    def test_comment_config(self, input, expected):
        assert CommentConfig(input).to_sql_clause() == expected


class TestCommentProcessor:
    def test_from_results__no_comment(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    [None, None, None],
                    ["# Detailed Table Information", None, None],
                    ["Catalog:", "default", None],
                    ["Schema:", "default", None],
                    ["Table:", "table_abc", None],
                ]
            )
        }
        config = CommentProcessor.from_results(results)
        assert config == CommentConfig()

    def test_from_results__with_comment(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_name", "data_type", "comment"],
                    ["col_a", "int", "This is a comment"],
                    [None, None, None],
                    ["# Detailed Table Information", None, None],
                    ["Catalog:", "default", None],
                    ["Schema:", "default", None],
                    ["Table:", "table_abc", None],
                    ["Comment", "This is the table comment", None],
                ]
            )
        }
        config = CommentProcessor.from_results(results)
        assert config == CommentConfig("This is the table comment")
