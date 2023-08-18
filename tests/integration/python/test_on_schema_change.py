from tests.integration.base import DBTIntegrationTest, use_profile
import os


class TestPythonOnSchemaChange(DBTIntegrationTest):
    @property
    def schema(self):
        return "python"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "vars": {
                "http_path": os.getenv(
                    "DBT_DATABRICKS_CLUSTER_HTTP_PATH",
                ),
                "location_root": "",
            },
        }

    def _test_adding_column(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run", "-s", "adding_column"])
        self.run_dbt(["run", "-s", "adding_column"])

        self.assertTablesEqual("adding_column", "expected_new_column")

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_adding_column()
