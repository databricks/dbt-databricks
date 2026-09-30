"""An updateable-component change (databricks_tags, column tags, refresh schedule) is
applied via an in-place ALTER, and the run still refreshes data the server does not
auto-refresh.

Proven against live state: the MV's creation timestamp never moves (no CREATE OR
REPLACE), rows inserted into the source appear after a tags-only change on a MANUAL MV
(the run's REFRESH still happens), and stay invisible when the change moves the MV onto
an EVERY schedule (no REFRESH).
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
class TestMaterializedViewUpdateableChangeAltersInPlace:
    """Walk one MV through each updateable component (tags, column tags, then refresh
    schedule), asserting each change applies in place and the MV's data is refreshed only
    while it is not auto-refreshed."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"mv_norebuild_seed.csv": fixtures.mv_norebuild_seed_csv}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "mv_norebuild.sql": fixtures.mv_norebuild_v1,
            "schema.yml": fixtures.mv_norebuild_schema_v1,
        }

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
    def _created(project):
        # A rebuild (CREATE OR REPLACE) moves the creation timestamp; ALTER and REFRESH don't.
        return project.run_sql(
            f"""
            SELECT created
            FROM {project.database}.information_schema.tables
            WHERE table_catalog = '{project.database}'
              AND table_schema = '{project.test_schema}'
              AND table_name = 'mv_norebuild'
            """,
            fetch="one",
        )[0]

    @staticmethod
    def _table_tags(project) -> set:
        rows = project.run_sql(
            f"""
            SELECT tag_name, tag_value
            FROM `system`.`information_schema`.`table_tags`
            WHERE catalog_name = '{project.database}'
              AND schema_name = '{project.test_schema}'
              AND table_name = 'mv_norebuild'
            """,
            fetch="all",
        )
        return {(row[0], row[1]) for row in rows}

    @staticmethod
    def _column_tags(project) -> set:
        rows = project.run_sql(
            f"""
            SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{project.database}'
              AND schema_name = '{project.test_schema}'
              AND table_name = 'mv_norebuild'
            """,
            fetch="all",
        )
        return {(row[0], row[1], row[2]) for row in rows}

    @staticmethod
    def _refresh_config(project, mv: BaseRelation):
        # get_relation_config also polls the DLT pipeline until any in-flight refresh
        # completes, so it doubles as a settle point before re-querying row counts.
        with util.get_connection(project.adapter):
            cfg = project.adapter.get_relation_config(mv)
        assert isinstance(cfg, MaterializedViewConfig)
        return cfg.config["refresh"]

    def test_updateable_change_alters_in_place(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_norebuild"])
        mv = self._mv(project)
        seed = self._seed(project)
        assert self._row_count(project, mv) == 2
        # MANUAL to start: the server never auto-refreshes it (no schedule).
        assert self._refresh_config(project, mv).mode.value == "manual"
        created = self._created(project)
        assert created is not None

        # Mutate the source AFTER the MV is materialized.
        project.run_sql(f"insert into {seed} values (3, 300)")
        # The insert really landed in the source...
        assert self._row_count(project, seed) == 3
        # ...yet the MV is still stale at 2 (not auto-refreshed).
        assert self._row_count(project, mv) == 2

        # 1) tags change on a MANUAL MV -> in-place ALTER SET TAGS, and the run still refreshes.
        util.write_file(fixtures.mv_norebuild_v2_tag_changed, "models", "mv_norebuild.sql")
        util.run_dbt(["run", "--models", "mv_norebuild"])
        self._refresh_config(project, mv)  # settle any in-flight op
        assert self._table_tags(project) == {("lifecycle", "a"), ("extra", "b")}
        assert self._created(project) == created, "tags-only change rebuilt the MV"
        assert self._row_count(project, mv) == 3, (
            "MV missed the post-create source insert on a tags-only change; a MANUAL MV"
            " must still be refreshed when the run applies an in-place ALTER"
        )

        # 2) column-tag change on a MANUAL MV -> in-place ALTER COLUMN SET TAGS + refresh.
        project.run_sql(f"insert into {seed} values (4, 400)")
        util.write_file(fixtures.mv_norebuild_schema_v2_column_tag_changed, "models", "schema.yml")
        util.run_dbt(["run", "--models", "mv_norebuild"])
        self._refresh_config(project, mv)  # settle any in-flight op
        assert self._column_tags(project) == {
            ("id", "pii", "false"),
            ("value", "pii", "true"),
        }
        assert self._created(project) == created, "column-tags-only change rebuilt the MV"
        assert self._row_count(project, mv) == 4, (
            "MV missed the post-create source insert on a column-tags-only change; a MANUAL"
            " MV must still be refreshed when the run applies an in-place ALTER"
        )

        # 3) refresh-schedule change (MANUAL -> EVERY 4 WEEKS) -> in-place ALTER only: the
        # MV is now auto-refreshed, so the run issues neither REFRESH nor CREATE OR REPLACE.
        project.run_sql(f"insert into {seed} values (5, 500)")
        util.write_file(fixtures.mv_norebuild_v3_refresh_changed, "models", "mv_norebuild.sql")
        util.run_dbt(["run", "--models", "mv_norebuild"])
        refresh = self._refresh_config(project, mv)
        assert refresh.mode.value == "every"
        assert self._created(project) == created, "refresh-schedule-only change rebuilt the MV"
        assert self._row_count(project, mv) == 4, (
            "MV picked up the post-change source insert on a move to an EVERY schedule;"
            " an auto-refreshed MV must apply the change via in-place ALTER only, not"
            " CREATE OR REPLACE/REFRESH"
        )
