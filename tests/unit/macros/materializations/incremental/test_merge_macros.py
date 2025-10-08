import pytest

from tests.unit.macros.base import MacroTestBase


class TestGetMergeSQL(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def render_update_set(
        self,
        template,
        update_columns=[],
        on_schema_change="ignore",
        source_columns=[],
        source_alias="src",
    ):
        return self.run_macro_raw(
            template,
            "get_merge_update_set",
            update_columns,
            on_schema_change,
            source_columns,
            source_alias,
        )

    def test_get_merge_update_set__update_columns(self, template):
        sql = self.render_update_set(
            template, update_columns=["a", "b", "c"], source_alias="s"
        )
        expected = "a = s.a, b = s.b, c = s.c"
        self.assert_sql_equal(sql, expected)

    def test_get_merge_update_set__update_columns_takes_priority(self, template):
        sql = self.render_update_set(
            template,
            update_columns=["a"],
            on_schema_change="append",
            source_columns=["a", "b"],
            # source_alias is default
        )
        expected = "a = src.a"
        self.assert_sql_equal(sql, expected)

    def test_get_merge_update_set__no_update_columns_and_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="ignore",
            source_columns=["a"],
            # source_alias is default
        )
        assert sql == "*"

    def test_get_merge_update_set__source_columns_and_not_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="append",
            source_columns=["a", "b"],
            source_alias="SRC",
        )
        expected = "a = SRC.a, b = SRC.b"
        self.assert_sql_equal(sql, expected)

    def render_insert(
        self, template, on_schema_change="ignore", source_columns=[], source_alias="src"
    ):
        return self.run_macro_raw(
            template,
            "get_merge_insert",
            on_schema_change,
            source_columns,
            source_alias,
        )

    def test_get_merge_insert__ignore_takes_priority(self, template):
        # source_alias is default to 'src'
        sql = self.render_insert(
            template, on_schema_change="ignore", source_columns=["a"]
        )
        assert sql == "*"

    def test_get_merge_insert__source_columns_and_not_ignore(self, template):
        # source_alias is default to 'src'
        sql = self.render_insert(
            template, on_schema_change="append", source_columns=["a", "b"]
        )
        expected = "(a, b) VALUES (src.a, src.b)"
        self.assert_sql_equal(sql, expected)
