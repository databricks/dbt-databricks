"""Functional coverage for the new streaming-table refresh modes (EVERY / TRIGGER ON UPDATE)."""

import pytest
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)
from tests.functional.adapter.streaming_tables import fixtures


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
        yield {"st_on_update_bare.sql": fixtures.streaming_table_on_update_bare}

    def test_on_update_bare_mode_roundtrip(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_on_update_bare"])

        refresh = _get_refresh_config(project, "st_on_update_bare")
        assert refresh.mode.value == "on_update"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableDropAndReadd:
    """Drop schedule (config removed) and re-add."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_drop_readd.sql": fixtures.streaming_table_cron_no_tz}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_drop_then_readd(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "st_drop_readd"])

        util.write_file(fixtures.streaming_table, "models", "st_drop_readd.sql")
        util.run_dbt(["run", "--models", "st_drop_readd"])

        refresh = _get_refresh_config(project, "st_drop_readd")
        assert refresh.mode.value == "manual"

        util.write_file(
            fixtures.streaming_table_with_every("2 HOURS"), "models", "st_drop_readd.sql"
        )
        util.run_dbt(["run", "--models", "st_drop_readd"])

        refresh = _get_refresh_config(project, "st_drop_readd")
        assert refresh.mode.value == "every"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableScheduleLifecycle:
    """Walks one ST through the realistic schedule lifecycle:
    MANUAL → CRON → ON_UPDATE rate-limited → EVERY → (non-refresh change) → MANUAL.
    Each transition asserts the post-state schedule via DESCRIBE EXTENDED."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_lifecycle.sql": fixtures.streaming_table}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_full_lifecycle(self, project):
        util.run_dbt(["seed"])

        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "manual"

        util.write_file(fixtures.streaming_table_cron_no_tz, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "cron"
        assert refresh.cron == "0 0 * * * ? *"
        assert refresh == RefreshConfig(cron="0 0 * * * ? *")

        util.write_file(
            fixtures.streaming_table_on_update_rate_limited, "models", "st_lifecycle.sql"
        )
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "on_update"
        assert refresh.at_most_every is not None
        assert "900" in refresh.at_most_every
        # '15 MINUTES' comes back as '900 SECOND'; the two must compare equal so an
        # unchanged re-run produces no spurious schedule diff.
        assert refresh == RefreshConfig(on_update=True, at_most_every="15 MINUTES")

        util.write_file(
            fixtures.streaming_table_with_every("2 HOURS"), "models", "st_lifecycle.sql"
        )
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "every"

        util.write_file(
            fixtures.streaming_table_every_with_tblproperties, "models", "st_lifecycle.sql"
        )
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "every"

        util.write_file(fixtures.streaming_table, "models", "st_lifecycle.sql")
        util.run_dbt(["run", "--models", "st_lifecycle"])
        refresh = _get_refresh_config(project, "st_lifecycle")
        assert refresh.mode.value == "manual"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableEveryAcceptedInputs:
    """Walk one streaming table through each EVERY unit the regex accepts (HOUR / DAY /
    WEEK) and confirm dbt round-trips each against a real warehouse. Uses
    `on_configuration_change: apply` so each iteration is an ALTER, not a fresh CREATE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"st_every_inputs.sql": fixtures.streaming_table_with_every("2 HOURS")}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_every_accepted_input_round_trips(self, project):
        util.run_dbt(["seed"])

        for every_value in fixtures.EVERY_ACCEPTED_INPUTS:
            util.write_file(
                fixtures.streaming_table_with_every(every_value),
                "models",
                "st_every_inputs.sql",
            )
            util.run_dbt(["run", "--models", "st_every_inputs"])
            refresh = _get_refresh_config(project, "st_every_inputs")
            expected = RefreshConfig(every=every_value)
            assert refresh == expected, (
                f"warehouse did not accept or correctly store every={every_value!r}:"
                f" got {refresh!r}"
            )
            assert expected.get_diff(refresh) is None, (
                f"every={every_value!r} produces a spurious diff on re-run:"
                f" {expected.get_diff(refresh)!r}"
            )
