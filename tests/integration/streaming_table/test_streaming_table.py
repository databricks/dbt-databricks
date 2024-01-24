from tests.integration.base import DBTIntegrationTest, use_profile
from dbt.contracts.results import RunResult, RunStatus
import pytest


class TestStreamingTable(DBTIntegrationTest):
    @property
    def schema(self):
        return "streaming_table"

    @property
    def models(self):
        return "models"

    def _test_streaming_table_base(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run", "--select", "+st"])

        self.assertTablesEqual("st", "expected1")

        # Streaming table is not updated automatically.
        self.run_dbt(["run", "--select", "base"])
        self.assertTablesEqual("st", "expected1")

        # Streaming table is updated after refresh.
        self.run_dbt(["run", "--select", "st"])
        self.assertTablesEqual("st", "expected2")

        # Streaming table is recreated with full refresh.
        self.run_dbt(["run", "--full-refresh", "--select", "st"])
        self.assertTablesEqual("st", "expected2")

    def _test_streaming_table_no_cdf(self):
        # The base table is not CDF, but that is no longer needed
        self.run_dbt(["seed"])
        self.run_dbt(["run", "--select", "+st_nocdf"], expect_pass=True)
        self.run_dbt(["run", "--select", "base"])
        self.run_dbt(["run", "--select", "+st_nocdf"], expect_pass=True)
        self.assertTablesEqual("st_nocdf", "expected2")

    def _test_streaming_table_based_on_view(self):
        # If the base is a view, we can refresh a streaming table, provided the source of the view
        # doesn't change.
        self.run_dbt(["seed"])
        self.run_dbt(["run", "--select", "+st_on_view"], expect_pass=True)
        self.assertTablesEqual("st_on_view", "expected1")
        self.run_dbt(["run", "--select", "base"])
        result = self.run_dbt_and_capture(["run", "--select", "+st_on_view"], expect_pass=False)
        expected_message = (
            "has been failed due to a non-append only streaming source, and can't be"
            " restarted until the affected table has been full refreshed"
        )
        assert expected_message in result[1]

    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_base_databricks_uc_sql_endpoint(self):
        self._test_streaming_table_base()

    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_no_cdf_databricks_uc_sql_endpoint(self):
        self._test_streaming_table_no_cdf()

    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_based_on_view_databricks_uc_sql_endpoint(self):
        self._test_streaming_table_based_on_view()
