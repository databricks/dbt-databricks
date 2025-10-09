import pytest

from tests.unit.macros.base import MacroTestBase


class TestOptimizeMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "optimize.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def test_macros_get_optimize_sql(self, config, template_bundle):
        config["zorder"] = "foo"
        sql = self.render_bundle(template_bundle, "get_optimize_sql")

        assert sql == "optimize `some_database`.`some_schema`.`some_table` zorder by (foo)"

    def test_macro_get_optimize_sql_multiple_args(self, config, template_bundle):
        config["zorder"] = ["foo", "bar"]
        sql = self.render_bundle(template_bundle, "get_optimize_sql")

        assert sql == "optimize `some_database`.`some_schema`.`some_table` zorder by (foo, bar)"

    def test_macros_optimize_with_extraneous_info(self, config, var, template_bundle):
        config["zorder"] = ["foo", "bar"]
        var["FOO"] = True
        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"

    @pytest.mark.parametrize("key_val", ["DATABRICKS_SKIP_OPTIMIZE", "databricks_skip_optimize"])
    def test_macros_optimize_with_skip(self, key_val, var, template_bundle):
        var[key_val] = True
        r = self.render_bundle(template_bundle, "optimize")

        assert r == ""
