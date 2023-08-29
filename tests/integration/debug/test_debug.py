from io import StringIO
from unittest import mock

from tests.integration.base import DBTIntegrationTest, use_profile


class TestDebug(DBTIntegrationTest):
    @property
    def schema(self):
        return "debug"

    @property
    def models(self):
        return "models"

    def run_and_test(self, contains_catalog: bool):
        with mock.patch("sys.stdout", new=StringIO()) as fake_out:
            self.run_dbt(["debug"])
        stdout = fake_out.getvalue()
        self.assertIn("host: ", stdout)
        self.assertIn("http_path: ", stdout)
        self.assertIn("schema: ", stdout)
        (self.assertIn if contains_catalog else self.assertNotIn)("catalog: ", stdout)

    @use_profile("databricks_cluster")
    def test_debug_databricks_cluster(self):
        self.run_and_test(contains_catalog=False)

    @use_profile("databricks_uc_cluster")
    def test_debug_databricks_uc_cluster(self):
        self.run_and_test(contains_catalog=True)

    @use_profile("databricks_uc_sql_endpoint")
    def test_debug_databricks_uc_sql_endpoint(self):
        self.run_and_test(contains_catalog=True)
