import pytest
from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalLiquidClustering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.liquid_clustering_a,
        }

    def test_changing_cluster_by(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.liquid_clustering_b, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "describe extended {database}.{schema}.merge_update_columns_sql",
            fetch="all",
        )
        for row in results:
            if row[0] == "clusteringColumns":
                assert row[1] == [["msg"], ["color"]]

        util.write_file(fixtures.liquid_clustering_c, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "describe extended {database}.{schema}.merge_update_columns_sql",
            fetch="all",
        )
        # Unset the clustering columns
        for row in results:
            if row[0] == "clusteringColumns":
                assert False


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalPythonLiquidClustering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": fixtures.simple_python_model,
            "schema.yml": fixtures.lc_python_schema,
        }

    def test_changing_cluster_by(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.lc_python_schema2, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "describe extended {database}.{schema}.simple_python_model",
            fetch="all",
        )
        for row in results:
            if row[0] == "clusteringColumns":
                assert row[1] == [["test2"]]
