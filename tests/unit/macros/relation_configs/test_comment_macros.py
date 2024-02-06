import pytest
from tests.unit.macros.relation_configs.base import RelationConfigTestBase


class TestCommentMacros(RelationConfigTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "comment.sql"

    def test_get_create_sql_comment__with_no_comment(self, template):
        s = template.get_create_sql_comment(None)
        assert s == ""

    def test_get_create_sql_comment__with_empty_comment(self, template):
        s = template.get_create_sql_comment("")
        assert s == "COMMENT ''"

    def test_get_create_sql_comment__with_comment(self, template):
        s = template.get_create_sql_comment("test_comment")
        assert s == "COMMENT 'test_comment'"
