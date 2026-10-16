from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from dbt.adapters.databricks.handle import SqlUtils
from tests.unit.macros.base import MacroTestBase


class TestColumnsMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters/columns.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/adapters"]

    def test_get_columns_comments_as_json_sql(self, template_bundle, relation):
        result = self.run_macro(
            template_bundle.template, "get_columns_comments_as_json_sql", relation
        )

        expected_sql = "DESCRIBE TABLE EXTENDED `some_database`.`some_schema`.`some_table` AS JSON"
        self.assert_sql_equal(result, expected_sql)

    def test_drop_columns_sql(self, template_bundle, context, relation):
        """Test drop_columns_sql macro"""
        # Mock Column.format_remove_column_list
        context["api"] = MagicMock()
        context["api"].Column.format_remove_column_list = Mock(return_value="col1, col2")

        remove_columns = ["col1", "col2"]

        result = self.run_macro(
            template_bundle.template, "drop_columns_sql", relation, remove_columns
        )

        expected_sql = (
            "ALTER TABLE `some_database`.`some_schema`.`some_table` DROP COLUMNS (col1, col2)"
        )
        self.assert_sql_equal(result, expected_sql)

    def test_add_columns_sql(self, template_bundle, context, relation):
        """Test add_columns_sql macro"""
        # Mock Column.format_add_column_list
        context["api"] = MagicMock()
        context["api"].Column.format_add_column_list = Mock(return_value="col1 INT, col2 STRING")

        add_columns = ["col1", "col2"]

        result = self.run_macro(template_bundle.template, "add_columns_sql", relation, add_columns)

        expected_sql = (
            "ALTER TABLE `some_database`.`some_schema`.`some_table` "
            "ADD COLUMNS (col1 INT, col2 STRING)"
        )
        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize("select_sql", ["select 1 as id", " select 1 as id;\n"])
    def test_get_empty_subquery_sql_strips_terminal_semicolon(
        self, template_bundle, context, select_sql
    ):
        context["adapter"].clean_sql = SqlUtils.clean_sql
        context["dbt"] = SimpleNamespace(
            default__get_empty_subquery_sql=lambda sql, header: f"wrapped({sql})"
        )

        result = self.run_macro(
            template_bundle.template, "databricks__get_empty_subquery_sql", select_sql
        )

        assert result == "wrapped(select 1 as id)"
