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

    def test_streaming_table_base(self):
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

    def test_streaming_table_no_cdf(self):
        # The base table is not CDF.
        result = self.run_dbt(["run", "--select", "+st_nocdf"], expect_pass=True)
        self.run_dbt(["run", "--select", "base"])
        result = self.run_dbt(["run", "--select", "+st_nocdf"], expect_pass=True)
        assert len(result.results) == 2
        res: RunResult = result.results[1]
        assert res.status == RunStatus.Error
        assert (
            res.message
            and "The input for materialized view must have Change Data Feed enabled." in res.message
        )

    def test_streaming_table_based_on_view(self):
        result = self.run_dbt(["run", "--select", "+st_on_view"], expect_pass=False)
        assert len(result.results) == 3
        res: RunResult = result.results[2]
        assert res.status == RunStatus.Error
        assert (
            res.message
            and " The input for materialized view cannot be based on views." in res.message
        )

    @pytest.mark.skip(reason="not yet ready for production")
    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_base_databricks_uc_sql_endpoint(self):
        self.test_streaming_table_base()

    @pytest.mark.skip(reason="not yet ready for production")
    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_no_cdf_databricks_uc_sql_endpoint(self):
        self.test_streaming_table_no_cdf()

    @pytest.mark.skip(reason="not yet ready for production")
    @use_profile("databricks_uc_sql_endpoint")
    def test_streaming_table_based_on_view_databricks_uc_sql_endpoint(self):
        self.test_streaming_table_based_on_view()
