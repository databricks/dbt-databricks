from types import SimpleNamespace

import pytest

from dbt.adapters.databricks.handle import SqlUtils
from tests.unit.macros.base import MacroTestBase


class TestUnitTestHelpersMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/tests", "macros"]

    def test_get_unit_test_sql_strips_terminal_semicolon(self, template, context):
        context["adapter"].clean_sql = SqlUtils.clean_sql
        context["dbt"] = SimpleNamespace(
            default__get_unit_test_sql=lambda main, expected, cols: f"{main}|{expected}|{cols}"
        )

        result = self.run_macro_raw(
            template, "databricks__get_unit_test_sql", "select 1 as id;\n", "select 1", ["id"]
        )

        assert result == "select 1 as id|select 1|['id']"
