"""
Unit tests for row filter Jinja macros.

These tests verify that the Jinja macros in row_filter.sql produce correct SQL
and handle edge cases properly. This complements the Python unit tests in
tests/unit/relation_configs/test_row_filter.py which test the Python-side logic.

The goal is to ensure parity between:
- Python: RowFilterProcessor._qualify_function_name()
- Jinja: qualify_row_filter_function()
"""

from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation import DatabricksRelationType
from tests.unit.macros.base import MacroTestBase


class TestQuoteRowFilterColumns(MacroTestBase):
    """Tests for the quote_row_filter_columns helper macro."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_single_column(self, template_bundle):
        sql = self.run_macro(template_bundle.template, "quote_row_filter_columns", ["col1"])
        assert sql == "`col1`"

    def test_multiple_columns(self, template_bundle):
        sql = self.run_macro(template_bundle.template, "quote_row_filter_columns", ["a", "b", "c"])
        assert sql == "`a`, `b`, `c`"

    def test_reserved_words(self, template_bundle):
        sql = self.run_macro(
            template_bundle.template, "quote_row_filter_columns", ["select", "order", "table"]
        )
        assert sql == "`select`, `order`, `table`"

    def test_column_with_spaces(self, template_bundle):
        sql = self.run_macro(
            template_bundle.template, "quote_row_filter_columns", ["my column", "another col"]
        )
        assert sql == "`my column`, `another col`"


class TestQuoteRowFilterFunction(MacroTestBase):
    """Tests for the quote_row_filter_function helper macro.

    This macro adds backticks to a raw function name at SQL generation time.
    Function names are stored raw internally (e.g., 'cat.schema.fn') and quoted
    only when generating SQL (e.g., '`cat`.`schema`.`fn`').
    """

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_three_part_function_name(self, template_bundle):
        """Three-part name should get each part quoted separately."""
        sql = self.run_macro(
            template_bundle.template, "quote_row_filter_function", "cat.schema.my_filter"
        )
        expected = "`cat`.`schema`.`my_filter`"
        assert sql == self.clean_sql(expected)

    def test_function_with_special_chars(self, template_bundle):
        """Function names with special characters should be properly quoted."""
        sql = self.run_macro(
            template_bundle.template, "quote_row_filter_function", "my_cat.my_schema.my_filter_v2"
        )
        expected = "`my_cat`.`my_schema`.`my_filter_v2`"
        assert sql == self.clean_sql(expected)

    def test_fallback_for_unexpected_format(self, template_bundle):
        """Non-3-part names should fallback to quoting the whole string."""
        sql = self.run_macro(
            template_bundle.template, "quote_row_filter_function", "just_a_function"
        )
        expected = "`just_a_function`"
        assert sql == self.clean_sql(expected)


class TestQualifyRowFilterFunction(MacroTestBase):
    """Tests for the qualify_row_filter_function macro.

    These tests should mirror the Python tests for _qualify_function_name()
    in tests/unit/relation_configs/test_row_filter.py.
    """

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_unqualified_function(self, template_bundle):
        """1-part name should be qualified with relation's database.schema."""
        sql = self.run_macro(
            template_bundle.template,
            "qualify_row_filter_function",
            "my_filter",
            template_bundle.relation,
        )
        expected = "some_database.some_schema.my_filter"
        assert sql == self.clean_sql(expected)

    def test_fully_qualified_function(self, template_bundle):
        """3-part name should be returned raw."""
        sql = self.run_macro(
            template_bundle.template,
            "qualify_row_filter_function",
            "cat.schema.fn",    
            template_bundle.relation,
        )
        expected = "cat.schema.fn"
        assert sql == self.clean_sql(expected)

    def test_function_with_existing_backticks(self, template_bundle):
        """Function name with backticks should have them stripped and returned raw."""
        sql = self.run_macro(
            template_bundle.template,
            "qualify_row_filter_function",
            "`cat`.`schema`.`fn`",
            template_bundle.relation,
        )
        expected = "cat.schema.fn"
        assert sql == self.clean_sql(expected)

    def test_two_part_function_raises_error(self, context, template_bundle):
        """2-part name is ambiguous and should raise an error."""
        context["exceptions"] = Mock()
        context["exceptions"].raise_compiler_error = Mock(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            self.run_macro(
                template_bundle.template,
                "qualify_row_filter_function",
                "schema.fn",
                template_bundle.relation,
            )

        # Verify the error message mentions ambiguity
        call_args = context["exceptions"].raise_compiler_error.call_args[0][0]
        assert "ambiguous" in call_args.lower()

    def test_four_part_function_raises_error(self, context, template_bundle):
        """4+ part name should raise an error."""
        context["exceptions"] = Mock()
        context["exceptions"].raise_compiler_error = Mock(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            self.run_macro(
                template_bundle.template,
                "qualify_row_filter_function",
                "a.b.c.d",
                template_bundle.relation,
            )

        # Verify the error message mentions too many parts
        call_args = context["exceptions"].raise_compiler_error.call_args[0][0]
        assert "too many parts" in call_args.lower()


class TestAlterSetRowFilter(MacroTestBase):
    """Tests for the alter_set_row_filter macro."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_basic_alter_set(self, template_bundle):
        """Test basic ALTER TABLE SET ROW FILTER."""
        row_filter = Mock()
        row_filter.function = "cat.schema.my_filter"
        row_filter.columns = ("col1",)

        sql = self.run_macro(
            template_bundle.template, "alter_set_row_filter", template_bundle.relation, row_filter
        )
        expected = (
            "alter table `some_database`.`some_schema`.`some_table` "
            "set row filter `cat`.`schema`.`my_filter` on (`col1`)"
        )
        assert sql == self.clean_sql(expected)

    def test_alter_set_multiple_columns(self, template_bundle):
        """Test ALTER with multiple columns."""
        row_filter = Mock()
        row_filter.function = "cat.schema.my_filter"
        row_filter.columns = ("region", "country_code")

        sql = self.run_macro(
            template_bundle.template, "alter_set_row_filter", template_bundle.relation, row_filter
        )
        expected = (
            "alter table `some_database`.`some_schema`.`some_table` "
            "set row filter `cat`.`schema`.`my_filter` on (`region`, `country_code`)"
        )
        assert sql == self.clean_sql(expected)

    def test_alter_set_on_materialized_view(self, template_bundle):
        """Test ALTER on materialized view."""
        template_bundle.relation.type = DatabricksRelationType.MaterializedView
        template_bundle.relation.type.render = Mock(return_value="MATERIALIZED VIEW")

        row_filter = Mock()
        row_filter.function = "cat.schema.my_filter"
        row_filter.columns = ("col1",)

        sql = self.run_macro(
            template_bundle.template, "alter_set_row_filter", template_bundle.relation, row_filter
        )
        assert "materialized view" in sql.lower()


class TestAlterDropRowFilter(MacroTestBase):
    """Tests for the alter_drop_row_filter macro."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_drop_row_filter_table(self, template_bundle):
        """Test DROP ROW FILTER on table."""
        sql = self.run_macro(
            template_bundle.template, "alter_drop_row_filter", template_bundle.relation
        )
        expected = (
            "alter table `some_database`.`some_schema`.`some_table` " "drop row filter"
        )
        assert sql == self.clean_sql(expected)

    def test_drop_row_filter_materialized_view(self, template_bundle):
        """Test DROP ROW FILTER on materialized view."""
        template_bundle.relation.type = DatabricksRelationType.MaterializedView
        template_bundle.relation.type.render = Mock(return_value="MATERIALIZED VIEW")

        sql = self.run_macro(
            template_bundle.template, "alter_drop_row_filter", template_bundle.relation
        )
        assert "materialized view" in sql.lower()
        assert "drop row filter" in sql.lower()


class TestGetCreateRowFilterClause(MacroTestBase):
    """Tests for the get_create_row_filter_clause macro."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_no_config(self, config, template_bundle):
        """Test with no row_filter config."""
        # config is empty by default
        sql = self.run_macro(
            template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
        )
        assert sql == ""

    def test_with_valid_config(self, config, template_bundle):
        """Test with valid row_filter config."""
        config["row_filter"] = {"function": "my_filter", "columns": ["region"]}

        sql = self.run_macro(
            template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
        )
        expected = (
            "with row filter `some_database`.`some_schema`.`my_filter` on (`region`)"
        )
        assert sql == self.clean_sql(expected)

    def test_with_multiple_columns(self, config, template_bundle):
        """Test with multiple columns."""
        config["row_filter"] = {
            "function": "multi_col_filter",
            "columns": ["region", "country_code"],
        }

        sql = self.run_macro(
            template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
        )
        assert "`region`, `country_code`" in sql.lower()

    def test_with_string_column_normalized(self, config, template_bundle):
        """Test that string columns value is normalized to list."""
        config["row_filter"] = {"function": "my_filter", "columns": "region"}

        sql = self.run_macro(
            template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
        )
        expected = (
            "with row filter `some_database`.`some_schema`.`my_filter` on (`region`)"
        )
        assert sql == self.clean_sql(expected)

    def test_with_fully_qualified_function(self, config, template_bundle):
        """Test with fully qualified function name."""
        config["row_filter"] = {
            "function": "other_cat.other_schema.other_filter",
            "columns": ["col1"],
        }

        sql = self.run_macro(
            template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
        )
        assert "`other_cat`.`other_schema`.`other_filter`" in sql.lower()

    def test_empty_columns_raises_error(self, config, context, template_bundle):
        """Test that empty columns raises an error."""
        config["row_filter"] = {"function": "my_filter", "columns": []}
        context["exceptions"] = Mock()
        context["exceptions"].raise_compiler_error = Mock(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            self.run_macro(
                template_bundle.template, "get_create_row_filter_clause", template_bundle.relation
            )

        # Verify the error message mentions non-empty columns
        call_args = context["exceptions"].raise_compiler_error.call_args[0][0]
        assert "non-empty" in call_args.lower() or "columns" in call_args.lower()


class TestFetchRowFiltersSql(MacroTestBase):
    """Tests for the fetch_row_filters_sql macro."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "row_filter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros"]

    def test_fetch_row_filters_sql(self, template_bundle):
        """Test the SQL generated for fetching row filters."""
        sql = self.run_macro(
            template_bundle.template, "fetch_row_filters_sql", template_bundle.relation
        )
        expected = """
        SELECT
          table_catalog,
          table_schema,
          table_name,
          filter_name,
          target_columns
        FROM `some_database`.`information_schema`.`row_filters`
        WHERE table_catalog = 'some_database'
          AND table_schema = 'some_schema'
          AND table_name = 'some_table'
        """
        assert sql == self.clean_sql(expected)
