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

    def test_get_insert_overwrite_sql_case_insensitive_logic(self, template):
        """Test that the column comparison logic is case insensitive."""
        # This is a simple integration test to verify the macro compiles
        # The real test is in the functional tests where actual database interactions happen
        # We just want to ensure our filter changes don't break the macro compilation
        try:
            # Try to render the macro - if our jinja filter changes broke something, it will fail
            sql = self.run_macro_raw(
                template,
                "insert_into_sql_impl",
                "target_relation",
                ["ID", "Name"],  # dest columns with mixed case
                "source_relation",
                ["id", "name"],  # source columns with different case
            )
            # If we get here, the macro compiled successfully with mixed case columns
            assert "insert into table" in sql
            assert "ID, Name" in sql
        except Exception as e:
            pytest.fail(f"Macro compilation failed with case-sensitive column names: {e}")

    def test_insert_into_sql_impl_case_insensitive(self, template):
        """Test that column comparisons in insert into are case insensitive."""
        # Test with mixed case destination and source columns
        dest_columns = ["ID", "Name", "AGE"]
        source_columns = ["id", "name", "age"]

        sql = self.run_macro_raw(
            template,
            "insert_into_sql_impl",
            "target_relation",
            dest_columns,
            "source_relation",
            source_columns,
        )

        expected = (
            "insert into table target_relation (ID, Name, AGE)\n"
            "select ID, Name, AGE from source_relation"
        )
        self.assert_sql_equal(sql, expected)
