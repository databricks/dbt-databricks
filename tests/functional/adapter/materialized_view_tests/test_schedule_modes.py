"""Functional coverage for the new MV refresh modes (EVERY / TRIGGER ON UPDATE)."""

import pytest
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from tests.functional.adapter.materialized_view_tests import fixtures

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
    schedule = {'cron': '0 0 * * * ? *'},
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
        assert refresh.mode.value == "every"

    def test_on_update_bare_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_on_update_bare"])

        refresh = _get_refresh_config(project, "mv_on_update_bare")
        assert refresh.mode.value == "on_update"

    def test_on_update_rate_limited_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_on_update_rate_limited"])

        refresh = _get_refresh_config(project, "mv_on_update_rate_limited")
        assert refresh.mode.value == "on_update"
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every


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


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewDropAndReadd:
    """Drop schedule (config removed) and re-add."""

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
        util.run_dbt(["run", "--models", "mv_drop_readd"])

        refresh = _get_refresh_config(project, "mv_drop_readd")
        assert refresh.mode.value == "manual"

        util.write_file(MV_EVERY_2_HOURS, "models", "mv_drop_readd.sql")
        util.run_dbt(["run", "--models", "mv_drop_readd"])

        refresh = _get_refresh_config(project, "mv_drop_readd")
        assert refresh.mode.value == "every"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewScheduleLifecycle:
    """Walks one MV through the realistic schedule lifecycle:
    MANUAL → CRON → ON_UPDATE rate-limited → EVERY → (non-refresh change) → MANUAL.
    Each transition asserts the post-state schedule via DESCRIBE EXTENDED."""

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

        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "manual"

        util.write_file(MV_CRON, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "cron"
        assert refresh.cron == "0 0 * * * ? *"
        assert refresh == RefreshConfig(cron="0 0 * * * ? *")

        util.write_file(MV_ON_UPDATE_RATE_LIMITED, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "on_update"
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every

        util.write_file(MV_EVERY_2_HOURS, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "every"

        util.write_file(MV_EVERY_WITH_TBLPROPS, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "every"

        util.write_file(MV_NO_SCHEDULE, "models", "mv_lifecycle.sql")
        util.run_dbt(["run", "--models", "mv_lifecycle"])
        refresh = _get_refresh_config(project, "mv_lifecycle")
        assert refresh.mode.value == "manual"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewEveryAcceptedInputs:
    """Walk one MV through each EVERY unit the regex accepts (HOUR / DAY / WEEK) and
    confirm dbt round-trips each against a real warehouse. Uses
    `on_configuration_change: apply` so each iteration is an ALTER, not a fresh CREATE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"mv_every_inputs.sql": fixtures.materialized_view_with_every("2 HOURS")}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_every_accepted_input_round_trips(self, project):
        util.run_dbt(["seed"])

        for every_value in fixtures.EVERY_ACCEPTED_INPUTS:
            util.write_file(
                fixtures.materialized_view_with_every(every_value),
                "models",
                "mv_every_inputs.sql",
            )
            util.run_dbt(["run", "--models", "mv_every_inputs"])
            refresh = _get_refresh_config(project, "mv_every_inputs")
            expected = RefreshConfig(every=every_value)
            assert refresh == expected, (
                f"warehouse did not accept or correctly store every={every_value!r}:"
                f" got {refresh!r}"
            )
            assert expected.get_diff(refresh) is None, (
                f"every={every_value!r} produces a spurious diff on re-run:"
                f" {expected.get_diff(refresh)!r}"
            )
