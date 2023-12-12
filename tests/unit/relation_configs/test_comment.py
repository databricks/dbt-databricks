import pytest
from dbt.adapters.databricks.relation_configs.comment import CommentConfig


class TestCommentConfig:
    @pytest.mark.parametrize(
        "input,expected", [(None, ""), ("", ""), ("comment", "COMMENT 'comment'")]
    )
    def test_comment_config(self, input, expected):
        assert CommentConfig(input).to_sql_clause() == expected
