import pytest

from tests.unit.macros.base import MacroTestBase


class TestCommentMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "comment.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]

    def test_get_create_sql_comment__plain_comment(self, template_bundle):
        result = self.run_macro(
            template_bundle.template, "get_create_sql_comment", "A simple comment"
        )
        self.assert_sql_equal(result, "comment 'a simple comment'")

    def test_get_create_sql_comment__escapes_single_quote(self, template_bundle):
        result = self.run_macro(template_bundle.template, "get_create_sql_comment", "Bob's model")
        self.assert_sql_equal(result, "comment 'bob\\'s model'")

    def test_get_create_sql_comment__escapes_multiple_single_quotes(self, template_bundle):
        result = self.run_macro(template_bundle.template, "get_create_sql_comment", "it's a 'test'")
        self.assert_sql_equal(result, "comment 'it\\'s a \\'test\\''")

    def test_get_create_sql_comment__none_emits_nothing(self, template_bundle):
        result = self.run_macro(template_bundle.template, "get_create_sql_comment", None)
        assert result == ""

    def test_get_create_sql_comment__escapes_backslash_before_quote(self, template_bundle):
        """A backslash right before an apostrophe must not merge with its escape."""
        result = self.run_macro(template_bundle.template, "get_create_sql_comment", r"Bob\'s view")
        self.assert_sql_equal(result, r"comment 'bob\\\'s view'")

    def test_get_create_sql_comment__escapes_windows_path_with_quote(self, template_bundle):
        result = self.run_macro(
            template_bundle.template, "get_create_sql_comment", r"C:\temp\'s view"
        )
        self.assert_sql_equal(result, r"comment 'c:\\temp\\\'s view'")

    def test_get_create_sql_comment__escapes_bare_backslash(self, template_bundle):
        """An unescaped backslash is read by Databricks as the start of an escape sequence."""
        result = self.run_macro(template_bundle.template, "get_create_sql_comment", r"C:\temp")
        self.assert_sql_equal(result, r"comment 'c:\\temp'")
