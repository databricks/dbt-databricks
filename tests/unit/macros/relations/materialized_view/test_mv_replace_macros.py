from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestReplaceMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/materialized_view"]

    @pytest.fixture
    def mock_target_relation(self):
        return Mock()

    def test_get_replace_materialized_view_sql_calls_create_macro(
        self, template_bundle, context, mock_target_relation
    ):
        expected_sql = """
        create or replace materialized view `test_database`.`test_schema`.`test_mv` as select 1
        """
        context["get_create_materialized_view_as_sql"] = Mock(return_value=expected_sql)

        result = self.run_macro(
            template_bundle.template,
            "databricks__get_replace_materialized_view_sql",
            mock_target_relation,
            "select 1",
        )

        context["get_create_materialized_view_as_sql"].assert_called_once_with(
            mock_target_relation, "select 1"
        )
        self.assert_sql_equal(result, expected_sql)
