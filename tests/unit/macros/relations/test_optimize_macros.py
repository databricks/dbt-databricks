from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestOptimizeMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "optimize.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    @pytest.fixture(autouse=True)
    def setup_adapter_mocks(self, default_context, context):
        default_context["adapter"].resolve_file_format.return_value = "delta"
        context["is_predictive_optimization_enabled"] = Mock(return_value=False)

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

    def test_macros_optimize_skips_auto_liquid_clustering(self, config, context, template_bundle):
        config["auto_liquid_cluster"] = True

        result = self.render_bundle(template_bundle, "optimize")

        assert result == ""
        context["is_predictive_optimization_enabled"].assert_not_called()

    def test_macros_optimize_skip_optimize_false_forces_auto_liquid_clustering(
        self, config, template_bundle
    ):
        config["auto_liquid_cluster"] = True
        config["skip_optimize"] = False

        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"

    @pytest.mark.parametrize("key_val", ["DATABRICKS_SKIP_OPTIMIZE", "databricks_skip_optimize"])
    def test_macros_optimize_with_skip(self, key_val, config, var, template_bundle):
        config["liquid_clustered_by"] = ["foo"]
        var[key_val] = True
        r = self.render_bundle(template_bundle, "optimize")

        assert r == ""

    @pytest.mark.parametrize(
        "cluster_key,cluster_val",
        [
            ("zorder", "foo"),
            ("liquid_clustered_by", ["foo"]),
            ("auto_liquid_cluster", True),
        ],
    )
    def test_macros_optimize_with_skip_optimize_config(
        self, cluster_key, cluster_val, config, template_bundle
    ):
        config[cluster_key] = cluster_val
        config["skip_optimize"] = True
        r = self.render_bundle(template_bundle, "optimize")

        assert r == ""

    def test_macros_optimize_skips_liquid_clustering_under_predictive_optimization(
        self, config, context, template_bundle
    ):
        config["liquid_clustered_by"] = ["foo"]
        context["is_predictive_optimization_enabled"].return_value = True

        result = self.render_bundle(template_bundle, "optimize")

        assert result == ""
        context["is_predictive_optimization_enabled"].assert_called_once_with(
            template_bundle.relation
        )

    def test_macros_optimize_runs_liquid_clustering_without_predictive_optimization(
        self, config, template_bundle
    ):
        config["liquid_clustered_by"] = ["foo"]

        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"

    def test_macros_optimize_skip_optimize_false_forces_optimize(
        self, config, context, template_bundle
    ):
        config["liquid_clustered_by"] = ["foo"]
        config["skip_optimize"] = False
        context["is_predictive_optimization_enabled"].return_value = True

        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"
        context["is_predictive_optimization_enabled"].assert_not_called()

    def test_macros_optimize_zorder_ignores_predictive_optimization(
        self, config, context, template_bundle
    ):
        config["zorder"] = ["foo"]
        context["is_predictive_optimization_enabled"].return_value = True

        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"
        context["is_predictive_optimization_enabled"].assert_not_called()

    def test_macros_optimize_external_table_skips_predictive_optimization_check(
        self, config, context, template_bundle
    ):
        config["liquid_clustered_by"] = ["foo"]
        config["location_root"] = "s3://bucket/path"
        context["is_predictive_optimization_enabled"].return_value = True

        result = self.render_bundle(template_bundle, "optimize")

        assert result == "run_optimize_stmt"
        context["is_predictive_optimization_enabled"].assert_not_called()
