from unittest.mock import MagicMock, Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestColumnsMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters/columns.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/adapters"]

    @pytest.fixture
    def mock_relation(self):
        relation = Mock()
        relation.database = "test_db"
        relation.schema = "test_schema"
        relation.identifier = "test_table"
        relation.render = Mock(return_value="`test_db`.`test_schema`.`test_table`")
        relation.is_delta = True
        return relation

    def test_get_columns_comments_sql(self, template_bundle, mock_relation):
        result = self.run_macro(template_bundle.template, "get_columns_comments_sql", mock_relation)

        expected_sql = "DESCRIBE TABLE `test_db`.`test_schema`.`test_table`"
        assert result == self.clean_sql(expected_sql)

    def test_repair_table_sql(self, template_bundle, mock_relation):
        result = self.run_macro(template_bundle.template, "repair_table_sql", mock_relation)

        expected_sql = "REPAIR TABLE `test_db`.`test_schema`.`test_table` SYNC METADATA"
        assert result == self.clean_sql(expected_sql)

    def test_get_columns_comments_via_information_schema_sql(self, template_bundle, mock_relation):
        result = self.run_macro(
            template_bundle.template,
            "get_columns_comments_via_information_schema_sql",
            mock_relation,
        )

        # Note the lowercase in the WHERE clause due to |lower filters
        expected_sql = """
            SELECT
              column_name,
              full_data_type,
              comment
            FROM `system`.`information_schema`.`columns`
            WHERE
              table_catalog = 'test_db' and
              table_schema = 'test_schema' and
              table_name = 'test_table'
        """

        assert result == self.clean_sql(expected_sql)

    def test_drop_columns_sql(self, template_bundle, context, mock_relation):
        """Test drop_columns_sql macro"""
        # Mock Column.format_remove_column_list
        context["api"] = MagicMock()
        context["api"].Column.format_remove_column_list = Mock(return_value="col1, col2")

        remove_columns = ["col1", "col2"]

        result = self.run_macro(
            template_bundle.template, "drop_columns_sql", mock_relation, remove_columns
        )

        expected_sql = "ALTER TABLE `test_db`.`test_schema`.`test_table` DROP COLUMNS (col1, col2)"
        assert self.clean_sql(result) == self.clean_sql(expected_sql)

    def test_add_columns_sql(self, template_bundle, context, mock_relation):
        """Test add_columns_sql macro"""
        # Mock Column.format_add_column_list
        context["api"] = MagicMock()
        context["api"].Column.format_add_column_list = Mock(return_value="col1 INT, col2 STRING")

        add_columns = ["col1", "col2"]

        result = self.run_macro(
            template_bundle.template, "add_columns_sql", mock_relation, add_columns
        )

        expected_sql = (
            "ALTER TABLE `test_db`.`test_schema`.`test_table` ADD COLUMNS (col1 INT, col2 STRING)"
        )
        assert self.clean_sql(result) == self.clean_sql(expected_sql)
