from tests.integration.base import DBTIntegrationTest, use_profile
import os
import pytest


@pytest.mark.skip(
    reason="Run manually. Test must start with the Python compute\
          resource in TERMINATED or TERMINATING state"
)
class TestPythonConfig(DBTIntegrationTest):
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
                "http_path": os.getenv("DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH"),
                "location_root": os.getenv("DBT_DATABRICKS_LOCATION_ROOT"),
            },
        }

    def python_exc(self):
        self.run_dbt(["build", "-s", "complex_config"])

    @use_profile("databricks_uc_sql_endpoint")
    def test_python_databricks_uc_sql_endpoint(self):
        self.python_exc()
