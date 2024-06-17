import pytest
from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalReplaceTable:
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
