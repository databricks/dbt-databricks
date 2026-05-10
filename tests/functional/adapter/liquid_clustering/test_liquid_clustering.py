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


class TestTableV2LiquidClustering:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_liquid_cluster.sql": fixtures.table_liquid_cluster_sql,
        }

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_liquid_clustering(self, project):
        util.run_dbt(["run"])
