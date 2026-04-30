"""Functional coverage for the new MV refresh modes (EVERY / TRIGGER ON UPDATE)
and the GH #1293 regression.
"""

import pytest
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)

MV_EVERY_2_HOURS = """
{{ config(
    materialized='materialized_view',
    schedule = {'every': '2 HOURS'},
) }}
select * from {{ ref('my_seed') }}
"""

MV_ON_UPDATE_BARE = """
{{ config(
    materialized='materialized_view',
    schedule = {'on_update': True},
) }}
select * from {{ ref('my_seed') }}
"""

MV_ON_UPDATE_RATE_LIMITED = """
{{ config(
    materialized='materialized_view',
    schedule = {'on_update': True, 'at_most_every': '15 MINUTES'},
) }}
select * from {{ ref('my_seed') }}
"""

MV_CRON = """
{{ config(
    materialized='materialized_view',
    schedule = {'cron': '0 0 * * * ? *', 'time_zone_value': 'Etc/UTC'},
) }}
select * from {{ ref('my_seed') }}
"""

MV_NO_SCHEDULE = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('my_seed') }}
"""

MV_EVERY_WITH_TBLPROPS = """
{{ config(
    materialized='materialized_view',
    schedule = {'every': '2 HOURS'},
    tblproperties={'lifecycle_marker': 'v1'},
) }}
select * from {{ ref('my_seed') }}
"""


def _get_refresh_config(project, identifier):
    relation = project.adapter.Relation.create(
        identifier=identifier,
        schema=project.test_schema,
        database=project.database,
        type=DatabricksRelationType.MaterializedView,
    )
    with util.get_connection(project.adapter):
        results = project.adapter.get_relation_config(relation)
    assert isinstance(results, MaterializedViewConfig)
    return results.config["refresh"]


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewScheduleModes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "mv_every.sql": MV_EVERY_2_HOURS,
            "mv_on_update_bare.sql": MV_ON_UPDATE_BARE,
            "mv_on_update_rate_limited.sql": MV_ON_UPDATE_RATE_LIMITED,
        }

    def test_every_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_every"])

        refresh = _get_refresh_config(project, "mv_every")
        assert refresh.every is not None
        # Server returns plural; our parser preserves the canonical form.
        assert refresh.mode.value == "every"
        assert refresh.cron is None
        assert refresh.at_most_every is None

    def test_on_update_bare_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_on_update_bare"])

        refresh = _get_refresh_config(project, "mv_on_update_bare")
        assert refresh.mode.value == "on_update"
        assert refresh.on_update is True
        assert refresh.at_most_every is None
        assert refresh.cron is None
        assert refresh.every is None

    def test_on_update_rate_limited_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_on_update_rate_limited"])

        refresh = _get_refresh_config(project, "mv_on_update_rate_limited")
        assert refresh.mode.value == "on_update"
        # Server returns "INTERVAL N SECOND"; parser stores as "<n> SECOND".
        assert refresh.at_most_every is not None
        # 15 MINUTES → 900 SECOND server-side.
        assert "900" in refresh.at_most_every

    def test_idempotent_run_no_alter_for_every_mode(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_every"])
        # Second run with no config change: should NOT emit any ALTER or REFRESH.
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", "mv_every"])
        # Auto-refresh suppression: REFRESH MATERIALIZED VIEW is skipped for every/on_update.
        assert "refresh materialized view" not in logs.lower()


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewManualMode:
    """Initial-create with no `schedule` config: relation should round-trip as MANUAL.
    The drop-and-readd test covers CRON → MANUAL transition; this covers fresh MANUAL."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_manual.sql": MV_NO_SCHEDULE}

    def test_manual_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_manual"])

        refresh = _get_refresh_config(project, "mv_manual")
        assert refresh.mode.value == "manual"
        assert refresh.cron is None
        assert refresh.every is None
        assert refresh.on_update is False
        assert refresh.at_most_every is None


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewCronAutoRefreshPositiveControl:
    """Positive control for the auto-REFRESH suppression conditional: CRON mode must still
    emit REFRESH MATERIALIZED VIEW on idempotent re-runs (existing behavior preserved)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_cron_idempotent.sql": MV_CRON}

    def test_cron_mode_idempotent_run_still_refreshes(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_cron_idempotent"])
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", "mv_cron_idempotent"])
        assert "refresh materialized view" in logs.lower()


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewDropAndReadd:
    """Drop schedule (config removed) → DROP SCHEDULE; re-add → ADD SCHEDULE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_drop_readd.sql": MV_CRON}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_drop_then_readd(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_drop_readd"])

        no_schedule = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('my_seed') }}
"""
        util.write_file(no_schedule, "models", "mv_drop_readd.sql")
        _, drop_logs = util.run_dbt_and_capture(["--debug", "run", "--models", "mv_drop_readd"])
        assert "drop schedule" in drop_logs.lower()

        refresh = _get_refresh_config(project, "mv_drop_readd")
        assert refresh.mode.value == "manual"

        util.write_file(MV_EVERY_2_HOURS, "models", "mv_drop_readd.sql")
        _, add_logs = util.run_dbt_and_capture(["--debug", "run", "--models", "mv_drop_readd"])
        assert "add schedule every" in add_logs.lower()

        refresh = _get_refresh_config(project, "mv_drop_readd")
        assert refresh.mode.value == "every"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewCronToEveryAlter:
    """Cross-mode ALTER (CRON → EVERY) preserves the relation, no rebuild."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_cross_mode.sql": MV_CRON}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_cron_to_every_alter(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_cross_mode"])

        refresh = _get_refresh_config(project, "mv_cross_mode")
        assert refresh.cron == "0 0 * * * ? *"

        util.write_file(MV_EVERY_2_HOURS, "models", "mv_cross_mode.sql")

        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", "mv_cross_mode"])

        refresh = _get_refresh_config(project, "mv_cross_mode")
        assert refresh.mode.value == "every"
        assert refresh.cron is None
        # Confirm an ALTER (not REPLACE) was applied.
        util.assert_message_in_logs("Applying ALTER to:", logs)


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewGH1293Regression:
    """GH #1293: a relation with a TRIGGER schedule (set via post_hook) breaks the parser
    on the next dbt run. The fix is to have a parser that recognizes the TRIGGER syntax."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_gh_1293.sql": MV_CRON}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_post_hook_alter_to_trigger_then_run_succeeds(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_gh_1293"])

        # External ALTER (simulates the post_hook path the original GH #1293 user took).
        relation = project.adapter.Relation.create(
            identifier="mv_gh_1293",
            schema=project.test_schema,
            database=project.database,
        )
        project.run_sql(
            f"ALTER MATERIALIZED VIEW {relation} ALTER TRIGGER ON UPDATE"
            " AT MOST EVERY INTERVAL 15 MINUTES"
        )

        # Pre-fix this raised "Could not parse schedule from description: ...".
        util.run_dbt(["run", "--models", "mv_gh_1293"])


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewScheduleLifecycle:
    """Walks one MV through the realistic schedule lifecycle: MANUAL → CRON → ON_UPDATE
    rate-limited → EVERY → (non-refresh change) → MANUAL. Each transition asserts the
    post-state via DESCRIBE EXTENDED. Step 5 (tblproperties change) triggers
    requires_full_refresh -> REPLACE path on MV; the schedule must survive the re-CREATE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_lifecycle.sql": MV_NO_SCHEDULE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_full_lifecycle(self, project):
        util.run_dbt(["seed"])

        # 1. MANUAL initial create
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "manual"

        # 2. MANUAL → CRON (in-place ALTER ADD SCHEDULE)
        util.write_file(MV_CRON, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "cron"
        assert refresh.cron == "0 0 * * * ? *"

        # 3. CRON → ON_UPDATE rate-limited (in-place ALTER TRIGGER)
        util.write_file(MV_ON_UPDATE_RATE_LIMITED, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "on_update"
        assert refresh.on_update is True
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every

        # 4. ON_UPDATE → EVERY (in-place ALTER SCHEDULE)
        util.write_file(MV_EVERY_2_HOURS, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "every"
        assert refresh.cron is None
        assert refresh.at_most_every is None

        # 5. EVERY + tblproperties (REPLACE path; schedule must survive re-CREATE)
        util.write_file(MV_EVERY_WITH_TBLPROPS, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "every", (
            "Schedule lost across REPLACE for tblproperties change"
        )

        # 6. EVERY → MANUAL (ALTER DROP SCHEDULE)
        util.write_file(MV_NO_SCHEDULE, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "manual", "Schedule not dropped on return to MANUAL"
