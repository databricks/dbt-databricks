import pytest

from tests.unit.macros.base import MacroTestBase


class TestEscapeMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "escape.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def test_escape_relation_literal_plain(self, template):
        sql = self.run_macro(template, "escape_relation_literal", "plain_table")
        assert sql == "plain_table"

    def test_escape_relation_literal_with_quote(self, template):
        sql = self.run_macro(template, "escape_relation_literal", "orders'eu")
        assert sql == "orders\\'eu"

    def test_escape_relation_literal_with_backslash(self, template):
        sql = self.run_macro(template, "escape_relation_literal", "orders\\eu")
        assert sql == "orders\\\\eu"

    def test_escape_relation_literal_with_quote_and_backslash(self, template):
        sql = self.run_macro(template, "escape_relation_literal", "orders'eu\\test")
        assert sql == "orders\\'eu\\\\test"

    def test_escape_relation_literal_with_multiple_specials(self, template):
        sql = self.run_macro(template, "escape_relation_literal", "a\\'b\\\\c'd")
        assert sql == "a\\\\\\'b\\\\\\\\c\\'d"

    def test_escape_relation_literal_none(self, template):
        sql = self.run_macro(template, "escape_relation_literal", None)
        assert sql == ""
