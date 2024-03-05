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
        self, template, update_columns=[], on_schema_change="ignore", source_columns=[]
    ):
        return self.run_macro_raw(
            template, "get_merge_update_set", update_columns, on_schema_change, source_columns
        )

    def test_get_merge_update_set__update_columns(self, template):
        sql = self.render_update_set(template, update_columns=["a", "b", "c"])
        expected = "a = DBT_INTERNAL_SOURCE.a, b = DBT_INTERNAL_SOURCE.b, c = DBT_INTERNAL_SOURCE.c"
        assert sql == expected

    def test_get_merge_update_set__update_columns_takes_priority(self, template):
        sql = self.render_update_set(
            template,
            update_columns=["a"],
            on_schema_change="append",
            source_columns=["a", "b"],
        )
        expected = "a = DBT_INTERNAL_SOURCE.a"
        assert sql == expected

    def test_get_merge_update_set__no_update_columns_and_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="ignore",
            source_columns=["a"],
        )
        assert sql == "*"

    def test_get_merge_update_set__source_columns_and_not_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="append",
            source_columns=["a", "b"],
        )
        expected = "a = DBT_INTERNAL_SOURCE.a, b = DBT_INTERNAL_SOURCE.b"
        assert sql == expected

    def render_insert(self, template, on_schema_change="ignore", source_columns=[]):
        return self.run_macro_raw(template, "get_merge_insert", on_schema_change, source_columns)

    def test_get_merge_insert__ignore_takes_priority(self, template):
        sql = self.render_insert(template, on_schema_change="ignore", source_columns=["a"])
        assert sql == "*"

    def test_get_merge_insert__source_columns_and_not_ignore(self, template):
        sql = self.render_insert(template, on_schema_change="append", source_columns=["a", "b"])
        expected = "(a, b) VALUES (DBT_INTERNAL_SOURCE.a, DBT_INTERNAL_SOURCE.b)"
        assert sql == expected


class TestGeInsertIntoSQL(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def render_insert_into(self, template, dest_columns=["a"], source_columns=["a"]):
        return self.run_macro_raw(
            template, "insert_into_sql_impl", "target", dest_columns, "source", source_columns
        )

    def test_insert_into_sql_impl__matching_columns(self, template):
        sql = self.render_insert_into(template)
        expected = "insert into table target (a)\nselect a from source"
        assert sql == expected

    def test_insert_into_sql_impl__target_has_extra_columns(self, template):
        sql = self.render_insert_into(template, dest_columns=["a", "b"], source_columns=["b"])
        expected = "insert into table target (a, b)\nselect NULL, b from source"
        assert sql == expected

    def test_insert_into_sql_impl__source_has_extra_columns(self, template):
        # This would only happen if on_schema_change is set to "ignore", as otherwise
        # source columns get added to target before this

        sql = self.render_insert_into(template, dest_columns=["b"], source_columns=["a", "b"])
        expected = "insert into table target (b)\nselect b from source"
        assert sql == expected
