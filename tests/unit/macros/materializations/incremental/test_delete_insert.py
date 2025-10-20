import pytest

from tests.unit.macros.base import MacroTestBase


class TestDeleteInsertMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def render_delete_insert(
        self,
        template,
        unique_key,
        on_schema_change,
        incremental_predicates=None,
        target_columns=("a", "b")
    ):
        return self.run_macro_raw(
            template,
            "delete_insert_sql_impl",
            "source",
            "target",
            target_columns,
            unique_key,
            on_schema_change,
            incremental_predicates
        )

    def test_delete_insert_sql_impl__single_unique_key(self, template):
        sql = self.render_delete_insert(template, "a", "sync_all_columns")
        expected = """
            insert into table target as target
            replace on (target.a <=> temp.a)
            (select a, b from source) as temp
            """
        self.assert_sql_equal(sql, expected)

    def test_delete_insert_sql_impl__multiple_unique_keys(self, template):
        sql = self.render_delete_insert(template, ["a", "b"], "sync_all_columns")
        expected = """
            insert into table target as target
            replace on (target.a <=> temp.a and target.b <=> temp.b)
            (select a, b from source) as temp
            """
        self.assert_sql_equal(sql, expected)

    def test_delete_insert_sql_impl__ignore_schema_change(self, template):
        sql = self.render_delete_insert(template, "a", "ignore")
        expected = """
            insert into table target as target
            replace on (target.a <=> temp.a)
            (select * from source) as temp
            """
        self.assert_sql_equal(sql, expected)

    def test_delete_insert_sql_impl__incremental_predicates(self, template):
        sql = self.render_delete_insert(template, "a", "ignore", "a > 1")
        expected = """
            insert into table target as target
            replace on (target.a <=> temp.a)
            (select * from source where a > 1) as temp
            """
        self.assert_sql_equal(sql, expected)

    def test_delete_insert_sql_impl__multiple_incremental_predicates(self, template):
        sql = self.render_delete_insert(template, "a", "ignore", ["a > 1", "b < 3"])
        expected = """
            insert into table target as target
            replace on (target.a <=> temp.a)
            (select * from source where a > 1 and b < 3) as temp
            """
        self.assert_sql_equal(sql, expected)
