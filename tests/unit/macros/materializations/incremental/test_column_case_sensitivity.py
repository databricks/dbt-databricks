import pytest

from tests.unit.macros.base import MacroTestBase


class TestColumnCaseSensitivity(MacroTestBase):
    """Test case sensitivity fixes for column name comparisons."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def test_insert_into_sql_impl_matching_columns(self, template):
        """Test that insert_into_sql_impl uses BY NAME when columns match."""
        # When columns match (case-insensitive), use simple BY NAME
        dest_columns = ["id", "name", "age"]
        source_columns = ["ID", "Name", "AGE"]  # Different case but same columns
        
        sql = self.run_macro_raw(
            template,
            "insert_into_sql_impl",
            "target_relation",
            dest_columns,
            "source_relation",
            source_columns,
        )

        # Verify the generated SQL uses BY NAME with select *
        expected = "insert into target_relation by name select * from source_relation"
        self.assert_sql_equal(sql, expected)

    def test_insert_into_sql_impl_mismatched_columns(self, template):
        """Test that insert_into_sql_impl handles mismatched columns correctly."""
        # Source has extra columns that target doesn't have
        dest_columns = ["id", "name"]
        source_columns = ["id", "name", "extra_col"]
        
        sql = self.run_macro_raw(
            template,
            "insert_into_sql_impl",
            "target_relation",
            dest_columns,
            "source_relation",
            source_columns,
        )

        # Should only select common columns (without BY NAME since columns don't match)
        expected = "insert into target_relation (id, name) select id, name from source_relation"
        self.assert_sql_equal(sql, expected)
