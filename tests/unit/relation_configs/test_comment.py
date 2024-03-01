from mock import Mock
from agate import Table
from dbt.adapters.databricks.relation_configs.comment import CommentConfig, CommentProcessor


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
        config = CommentProcessor.from_relation_results(results)
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
        config = CommentProcessor.from_relation_results(results)
        assert config == CommentConfig(comment="This is the table comment")

    def test_from_model_node__no_comment(self):
        model_node = Mock()
        model_node.description = None
        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig()

    def test_from_model_node__empty_comment(self):
        model_node = Mock()
        model_node.description = ""
        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig(comment="")

    def test_from_model_node__comment(self):
        model_node = Mock()
        model_node.description = "a comment"
        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig(comment="a comment")
