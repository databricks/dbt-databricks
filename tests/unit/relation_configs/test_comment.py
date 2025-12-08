from unittest.mock import Mock

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
                ],
                column_names=["col_name", "data_type", "comment"],
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
                ],
                column_names=["col_name", "data_type", "comment"],
            )
        }
        config = CommentProcessor.from_relation_results(results)
        assert config == CommentConfig(comment="This is the table comment")

    def test_from_model_node__no_comment(self):
        model_node = Mock()
        model_node.description = None
        model_node.config.persist_docs = {"relation": True, "columns": False}
        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig(persist=True)

    def test_from_model_node__empty_comment(self):
        model_node = Mock()
        model_node.description = ""
        model_node.config.persist_docs = {}

        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig(comment=None, persist=False)

    def test_from_model_node__comment(self):
        model_node = Mock()
        model_node.description = "a comment"
        model_node.config.persist_docs = {"relation": True, "columns": False}

        config = CommentProcessor.from_relation_config(model_node)
        assert config == CommentConfig(comment="a comment", persist=True)


class TestCommentConfig:
    def test_comment_config_get_diff__no_changes(self):
        current = CommentConfig(comment="test comment")
        target = CommentConfig(comment="test comment", persist=True)
        assert target.get_diff(current) is None

    def test_comment_config_get_diff__comment_changed(self):
        current = CommentConfig(comment="old comment")
        target = CommentConfig(comment="new comment", persist=True)
        assert target.get_diff(current) == target

    def test_comment_config_get_diff__no_persist(self):
        current = CommentConfig(comment="test comment")
        target = CommentConfig(comment="test comment", persist=False)
        assert target.get_diff(current) is None
