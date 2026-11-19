import pytest
from dbt.tests import util

from tests.functional.adapter.liquid_clustering import fixtures


class TestAutoLiquidClustering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "auto_liquid_clustering.sql": fixtures.auto_liquid_cluster_sql,
        }

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_liquid_clustering(self, project):
        util.run_dbt(["run"])
        properties = project.run_sql(
            "show tblproperties {database}.{schema}.auto_liquid_clustering",
            fetch="all",
        )
        assert ("clusterByAuto", "true") in [(row[0], row[1]) for row in properties]


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
