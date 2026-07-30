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
