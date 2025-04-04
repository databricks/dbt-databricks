import pytest

from tests.unit.macros.base import MacroTestBase


class TestInsertIntoMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def render_insert_into(self, template, dest_columns=["a"], source_columns=["a"]):
        return self.run_macro_raw(
            template,
            "insert_into_sql_impl",
            "target",
            dest_columns,
            "source",
            source_columns,
        )

    def test_insert_into_sql_impl__matching_columns(self, template):
        sql = self.render_insert_into(template)
        expected = "insert into table target (a)\nselect a from source"
        self.assert_sql_equal(sql, expected)

    def test_insert_into_sql_impl__target_has_extra_columns(self, template):
        sql = self.render_insert_into(template, dest_columns=["a", "b"], source_columns=["b"])
        expected = "insert into table target (a, b)\nselect DEFAULT, b from source"
        self.assert_sql_equal(sql, expected)

    def test_insert_into_sql_impl__source_has_extra_columns(self, template):
        # This would only happen if on_schema_change is set to "ignore", as otherwise
        # source columns get added to target before this

        sql = self.render_insert_into(template, dest_columns=["b"], source_columns=["a", "b"])
        expected = "insert into table target (b)\nselect b from source"
        self.assert_sql_equal(sql, expected)
