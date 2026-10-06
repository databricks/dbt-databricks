import pytest

from tests.unit.macros.base import MacroTestBase


class TestFetchConstraintsMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "constraints.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros/relations", "macros"]

    def test_fetch_foreign_key_constraints_sql_scopes_joins_to_constraint_schema(
        self, template_bundle
    ):
        # Constraint names are unique only per schema; same-named FKs elsewhere must not join in.
        sql = self.render_bundle(template_bundle, "fetch_foreign_key_constraints_sql")
        expected = """
            SELECT
              kcu.constraint_name,
              kcu.column_name AS from_column,
              ukcu.table_catalog AS to_catalog,
              ukcu.table_schema AS to_schema,
              ukcu.table_name AS to_table,
              ukcu.column_name AS to_column
            FROM `some_database`.information_schema.key_column_usage kcu
            JOIN `some_database`.information_schema.referential_constraints rc
              ON kcu.constraint_catalog = rc.constraint_catalog
              AND kcu.constraint_schema = rc.constraint_schema
              AND kcu.constraint_name = rc.constraint_name
            JOIN `some_database`.information_schema.key_column_usage ukcu
              ON rc.unique_constraint_catalog = ukcu.constraint_catalog
              AND rc.unique_constraint_schema = ukcu.constraint_schema
              AND rc.unique_constraint_name = ukcu.constraint_name
              AND kcu.ordinal_position = ukcu.ordinal_position
            WHERE kcu.table_catalog = 'some_database'
              AND kcu.table_schema = 'some_schema'
              AND kcu.table_name = 'some_table'
              AND kcu.constraint_name IN (
                SELECT constraint_name
                FROM `some_database`.information_schema.table_constraints
                WHERE table_catalog = 'some_database'
                  AND table_schema = 'some_schema'
                  AND table_name = 'some_table'
                  AND constraint_type = 'FOREIGN KEY'
              )
            ORDER BY kcu.ordinal_position;
        """
        self.assert_sql_equal(sql, expected)
