import pytest
from dbt.tests import util

from tests.functional.adapter.liquid_clustering import fixtures


class TestLiquidClustering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "liquid_clustering.sql": fixtures.liquid_cluster_sql,
        }

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_liquid_clustering(self, project):
        _, logs = util.run_dbt_and_capture(["--debug", "run"])
        assert "optimize" in logs


class TestAutoLiquidClustering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "liquid_clustering.sql": fixtures.liquid_cluster_sql,
        }

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_liquid_clustering(self, project):
        _, logs = util.run_dbt_and_capture(["--debug", "run"])
        assert "optimize" in logs
