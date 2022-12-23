from tests.integration.base import DBTIntegrationTest, use_profile


class TestMaterializedView(DBTIntegrationTest):
    @property
    def schema(self):
        return "materialized_view"

    @property
    def models(self):
        return "models"

    def test_materialized_view(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

        self.assertTablesEqual("mv", "expected_1")

        # Materialized View is not updated automatically.
        self.run_dbt(["run", "--select", "base"])
        self.assertTablesEqual("mv", "expected_1")

        # Materialized View is updated after refresh.
        self.run_dbt(["run", "--select", "mv"])
        self.assertTablesEqual("mv", "expected_2")

        # Materialized View is recreated with full refresh.
        self.run_dbt(["run", "--full-refresh", "--select", "mv"])
        self.assertTablesEqual("mv", "expected_2")

    @use_profile("databricks_uc_sql_endpoint")
    def test_materialized_view_databricks_uc_sql_endpoint(self):
        self.test_materialized_view()
