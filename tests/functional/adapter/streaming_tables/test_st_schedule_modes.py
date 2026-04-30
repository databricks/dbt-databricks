"""Functional coverage for the new streaming-table refresh modes (EVERY / TRIGGER ON UPDATE),
plus DROP/re-ADD and CRON-mode auto-REFRESH positive control. Mirrors the MV suite."""

import pytest
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)

ST_EVERY_2_HOURS = """
{{ config(
    materialized='streaming_table',
    schedule = {'every': '2 HOURS'},
) }}
select * from stream {{ ref('my_seed') }}
"""

ST_ON_UPDATE_BARE = """
{{ config(
    materialized='streaming_table',
    schedule = {'on_update': True},
) }}
select * from stream {{ ref('my_seed') }}
"""

ST_ON_UPDATE_RATE_LIMITED = """
{{ config(
    materialized='streaming_table',
    schedule = {'on_update': True, 'at_most_every': '15 MINUTES'},
) }}
select * from stream {{ ref('my_seed') }}
"""

ST_CRON = """
{{ config(
    materialized='streaming_table',
    schedule = {'cron': '0 0 * * * ? *', 'time_zone_value': 'Etc/UTC'},
) }}
select * from stream {{ ref('my_seed') }}
"""

ST_EVERY_WITH_TBLPROPS = """
{{ config(
    materialized='streaming_table',
    schedule = {'every': '2 HOURS'},
    tblproperties={'lifecycle_marker': 'v1'},
) }}
select * from stream {{ ref('my_seed') }}
"""

ST_NO_SCHEDULE = """
{{ config(materialized='streaming_table') }}
select * from stream {{ ref('my_seed') }}
"""


def _get_refresh_config(project, identifier):
    relation = project.adapter.Relation.create(
        identifier=identifier,
        schema=project.test_schema,
        database=project.database,
        type=DatabricksRelationType.StreamingTable,
    )
    with util.get_connection(project.adapter):
        results = project.adapter.get_relation_config(relation)
    assert isinstance(results, StreamingTableConfig)
    return results.config["refresh"]


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableScheduleModes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "st_every.sql": ST_EVERY_2_HOURS,
            "st_on_update_bare.sql": ST_ON_UPDATE_BARE,
            "st_on_update_rate_limited.sql": ST_ON_UPDATE_RATE_LIMITED,
        }

    def test_every_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_every"])

        refresh = _get_refresh_config(project, "st_every")
        assert refresh.mode.value == "every"
        assert refresh.cron is None

    def test_on_update_bare_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_on_update_bare"])

        refresh = _get_refresh_config(project, "st_on_update_bare")
        assert refresh.mode.value == "on_update"
        assert refresh.on_update is True
        assert refresh.at_most_every is None

    def test_on_update_rate_limited_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_on_update_rate_limited"])

        refresh = _get_refresh_config(project, "st_on_update_rate_limited")
        assert refresh.mode.value == "on_update"
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every

    def test_idempotent_run_no_refresh_for_every_mode(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_every"])
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", "st_every"])
        assert "refreshing streaming table" not in logs.lower()


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableManualMode:
    """Initial-create with no `schedule` config: relation should round-trip as MANUAL.
    The drop-and-readd test covers CRON → MANUAL transition; this covers fresh MANUAL."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_manual.sql": ST_NO_SCHEDULE}

    def test_manual_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_manual"])

        refresh = _get_refresh_config(project, "st_manual")
        assert refresh.mode.value == "manual"
        assert refresh.cron is None
        assert refresh.every is None
        assert refresh.on_update is False
        assert refresh.at_most_every is None


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableCronAutoRefreshPositiveControl:
    """Positive control: CRON-mode ST still emits REFRESH on idempotent run."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_cron_idempotent.sql": ST_CRON}

    def test_cron_mode_idempotent_run_still_refreshes(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_cron_idempotent"])
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", "st_cron_idempotent"])
        assert "refreshing streaming table" in logs.lower()


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableDropAndReadd:
    """Drop ST schedule (config removed) and re-add. CREATE OR REFRESH clears the existing
    schedule on every run, so drop is implicit (no ALTER emitted) and re-add uses ADD."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_drop_readd.sql": ST_CRON}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_drop_then_readd(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_drop_readd"])

        no_schedule = """
{{ config(materialized='streaming_table') }}
select * from stream {{ ref('my_seed') }}
"""
        util.write_file(no_schedule, "models", "st_drop_readd.sql")
        _, drop_logs = util.run_dbt_and_capture(["--debug", "run", "--models", "st_drop_readd"])

        refresh = _get_refresh_config(project, "st_drop_readd")
        assert refresh.mode.value == "manual"

        util.write_file(ST_EVERY_2_HOURS, "models", "st_drop_readd.sql")
        _, add_logs = util.run_dbt_and_capture(["--debug", "run", "--models", "st_drop_readd"])
        assert "add schedule every" in add_logs.lower()

        refresh = _get_refresh_config(project, "st_drop_readd")
        assert refresh.mode.value == "every"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableScheduleLifecycle:
    """Walks one ST through the realistic schedule lifecycle: MANUAL → CRON → ON_UPDATE
    rate-limited → EVERY → (non-refresh change) → MANUAL. Each transition asserts the
    post-state via DESCRIBE EXTENDED. Step 5 (tblproperties change with schedule unchanged)
    exercises the streaming_table.py:62 fix -- CREATE OR REFRESH clears the schedule as a
    side effect, so the alter macro must re-ADD it even when refresh itself didn't diff."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_lifecycle.sql": ST_NO_SCHEDULE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_full_lifecycle(self, project):
        util.run_dbt(["seed"])

        # 1. MANUAL initial create
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "manual"

        # 2. MANUAL → CRON
        util.write_file(ST_CRON, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "cron"
        assert refresh.cron == "0 0 * * * ? *"

        # 3. CRON → ON_UPDATE rate-limited
        util.write_file(ST_ON_UPDATE_RATE_LIMITED, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "on_update"
        assert refresh.on_update is True
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every

        # 4. ON_UPDATE → EVERY
        util.write_file(ST_EVERY_2_HOURS, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "every"
        assert refresh.cron is None
        assert refresh.at_most_every is None

        # 5. EVERY + tblproperties (non-refresh change must preserve schedule)
        util.write_file(ST_EVERY_WITH_TBLPROPS, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "every", (
            "Schedule silently dropped when only tblproperties changed"
        )

        # 6. EVERY → MANUAL
        util.write_file(ST_NO_SCHEDULE, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "manual", "Schedule not dropped on return to MANUAL"
