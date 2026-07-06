import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalReplaceTable(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("model",)

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": fixtures.replace_table}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures.replace_expected}

    # Validate that when we replace an existing table, no extra partitions are left behind
    def test_replace(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.replace_incremental, "models", "model.sql")
        util.run_dbt(["run", "--full-refresh"])
        util.check_relations_equal(project.adapter, ["model", "seed"])


class FullRefreshRecreateBase(RerunSafeMixin):
    """`--full-refresh` recreates an existing incremental relation from its create
    branch. Append accumulates extra rows first, so the erased rows prove the recreate."""

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("full_refresh_recreate",)

    @pytest.fixture(scope="class")
    def models(self):
        return {"full_refresh_recreate.sql": fixtures.base_model}

    def test_full_refresh_recreates_existing_relation(self, project):
        # Create {1: hello, 2: goodbye}.
        util.run_dbt(["run"])
        # Incremental (append) run accumulates {2: yo, 3: anyway}.
        util.run_dbt(["run"])
        accumulated = project.run_sql(
            "select id, msg from full_refresh_recreate order by id, msg", fetch="all"
        )
        # Four rows incl. duplicate id=2 — there is real state for a recreate to erase.
        assert accumulated == [(1, "hello"), (2, "goodbye"), (2, "yo"), (3, "anyway")]

        # --full-refresh renders the non-incremental branch and recreates the table.
        util.run_dbt(["run", "--full-refresh"])
        recreated = project.run_sql(
            "select id, msg from full_refresh_recreate order by id, msg", fetch="all"
        )
        # Only the create-branch rows remain; the accumulated rows are gone.
        assert recreated == [(1, "hello"), (2, "goodbye")]


class TestIncrementalFullRefreshRecreate(FullRefreshRecreateBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "append"}}


class TestIncrementalFullRefreshRecreateV2(FullRefreshRecreateBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+incremental_strategy": "append"},
        }
