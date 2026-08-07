"""An updateable-component change (databricks_tags, refresh schedule) is applied via
an in-place ALTER and does not rebuild the materialized view.

Proven against live state via source-row staleness: a row inserted into the source
after the MV is materialized stays invisible across an updateable-only change. An
in-place ALTER neither recomputes the query nor issues a REFRESH; a full CREATE OR
REPLACE (or a REFRESH) would pull the new row in.
"""

import pytest
from dbt.adapters.base import BaseRelation
from dbt.tests import util

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from tests.functional.adapter.materialized_view_tests import fixtures


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewUpdateableChangeAltersWithoutRebuild:
    """Walk one MV through each updateable component (tags, then refresh schedule),
    asserting each change applies via in-place ALTER and leaves the MV's data untouched."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"mv_norebuild_seed.csv": fixtures.mv_norebuild_seed_csv}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_norebuild.sql": fixtures.mv_norebuild_v1}

    @staticmethod
    def _mv(project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="mv_norebuild",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.MaterializedView,
        )

    @staticmethod
    def _seed(project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="mv_norebuild_seed",
            schema=project.test_schema,
            database=project.database,
        )

    @staticmethod
    def _row_count(project, relation: BaseRelation) -> int:
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    @staticmethod
    def _refresh_config(project, mv: BaseRelation):
        # get_relation_config also polls the DLT pipeline until any in-flight refresh
        # completes, so it doubles as a settle point before re-querying row counts.
        with util.get_connection(project.adapter):
            cfg = project.adapter.get_relation_config(mv)
        assert isinstance(cfg, MaterializedViewConfig)
        return cfg.config["refresh"]

    def test_updateable_change_does_not_rebuild(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_norebuild"])
        mv = self._mv(project)
        seed = self._seed(project)
        assert self._row_count(project, mv) == 2
        # MANUAL to start: the server never auto-refreshes it (no schedule).
        assert self._refresh_config(project, mv).mode.value == "manual"

        # Mutate the source AFTER the MV is materialized.
        project.run_sql(f"insert into {seed} values (3, 300)")
        # The insert really landed in the source (so a rebuild/refresh WOULD show 3)...
        assert self._row_count(project, seed) == 3
        # ...yet the MV is still stale at 2 (not auto-refreshed).
        assert self._row_count(project, mv) == 2

        # 1) tags change -> in-place ALTER SET TAGS, no rebuild.
        util.write_file(fixtures.mv_norebuild_v2_tag_changed, "models", "mv_norebuild.sql")
        util.run_dbt(["run", "--models", "mv_norebuild"])
        self._refresh_config(project, mv)  # settle any in-flight op
        assert self._row_count(project, mv) == 2, (
            "MV picked up the post-create source insert on a tags-only change; an"
            " updateable change must apply via in-place ALTER, not CREATE OR REPLACE/REFRESH"
        )

        # 2) refresh-schedule change (MANUAL -> EVERY 4 WEEKS) -> in-place ALTER, no rebuild.
        util.write_file(fixtures.mv_norebuild_v3_refresh_changed, "models", "mv_norebuild.sql")
        util.run_dbt(["run", "--models", "mv_norebuild"])
        refresh = self._refresh_config(project, mv)
        # Effect: the schedule changed.
        assert refresh.mode.value == "every"
        # No rebuild: the post-create source insert is still invisible.
        assert self._row_count(project, mv) == 2, (
            "MV picked up the post-create source insert on a refresh-schedule-only change;"
            " an updateable change must apply via in-place ALTER, not CREATE OR REPLACE"
        )
