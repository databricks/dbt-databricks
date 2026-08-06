"""Schema evolution for materialized views (issue #1359)."""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.materialized_view_tests import fixtures


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewSchemaEvolution(RerunSafeMixin):
    """Upstream ``select *`` column adds must recreate the MV, not REFRESH."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_evolution_base.sql": fixtures.schema_evolution_base_v1_sql,
            "schema_evolution_mv.sql": fixtures.schema_evolution_mv_sql,
            "schema_evolution_mv.yml": fixtures.schema_evolution_mv_yml,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("schema_evolution_mv", "schema_evolution_base")

    def test_upstream_column_add_recreates_without_full_refresh_flag(self, project):
        util.run_dbt(["run"])
        util.write_file(
            fixtures.schema_evolution_base_v2_sql, "models", "schema_evolution_base.sql"
        )

        util.run_dbt(["run"])
        rows = project.run_sql("select id, name, new_column from schema_evolution_mv", fetch="all")
        assert rows == [(1, "foo", 42)]

    def test_column_add_to_properties_yaml_recreates(self, project):
        """The issue's exact sequence: upstream gains a column, then the YAML follows."""
        util.run_dbt(["run"])
        util.write_file(
            fixtures.schema_evolution_base_v2_sql, "models", "schema_evolution_base.sql"
        )
        util.write_file(fixtures.schema_evolution_mv_yml_v2, "models", "schema_evolution_mv.yml")

        util.run_dbt(["run"])
        rows = project.run_sql("select id, name, new_column from schema_evolution_mv", fetch="all")
        assert rows == [(1, "foo", 42)]


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewSchemaDriftOnConfigurationChangeFail(RerunSafeMixin):
    """Schema drift is a configuration change, so `fail` must stop the run."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_evolution_base.sql": fixtures.schema_evolution_base_v1_sql,
            "schema_evolution_mv.sql": fixtures.schema_evolution_mv_fail_sql,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("schema_evolution_mv", "schema_evolution_base")

    def test_drift_fails_and_leaves_mv_untouched(self, project):
        util.run_dbt(["run"])
        util.write_file(
            fixtures.schema_evolution_base_v2_sql, "models", "schema_evolution_base.sql"
        )

        util.run_dbt(["run"], expect_pass=False)

        rows = project.run_sql("select * from schema_evolution_mv", fetch="all")
        assert rows == [(1, "foo")]


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewSchemaDriftOnConfigurationChangeContinue(RerunSafeMixin):
    """Schema drift is a configuration change, so `continue` must skip the rebuild."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_evolution_base.sql": fixtures.schema_evolution_base_v1_sql,
            "schema_evolution_mv.sql": fixtures.schema_evolution_mv_continue_sql,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("schema_evolution_mv", "schema_evolution_base")

    def test_drift_continues_and_leaves_mv_untouched(self, project):
        util.run_dbt(["run"])
        util.write_file(
            fixtures.schema_evolution_base_v2_sql, "models", "schema_evolution_base.sql"
        )

        util.run_dbt(["run"])

        rows = project.run_sql("select * from schema_evolution_mv", fetch="all")
        assert rows == [(1, "foo")]
