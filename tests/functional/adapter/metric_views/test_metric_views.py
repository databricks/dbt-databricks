import pytest

from dbt.tests.util import run_dbt


# Extremely simple test to ensure model runs work in the presence of metric views
@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViews:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_model.sql": "select 1 as id"}

    def test_model_with_metric_view(self, project):
        # First create a source table
        project.run_sql(f"""
            CREATE TABLE {project.database}.{project.test_schema}.source_table (
                id INT,
                value INT
            )
        """)

        # Create a minimal metric view
        project.run_sql(f"""
            CREATE OR REPLACE VIEW {project.database}.{project.test_schema}.test_metric_view
            WITH METRICS
            LANGUAGE YAML
            AS $$
            version: 0.1
            source: {project.database}.{project.test_schema}.source_table
            dimensions:
            - name: id
              expr: id
            measures:
            - name: count
              expr: count(1)
            $$;
        """)

        # Run a regular dbt model
        results = run_dbt(["run"])
        assert len(results) == 1
