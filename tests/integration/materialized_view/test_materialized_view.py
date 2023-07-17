from tests.integration.base import DBTIntegrationTest, use_profile
from dbt.contracts.results import RunResult, RunStatus
import pytest


class TestMaterializedView(DBTIntegrationTest):
    @property
    def schema(self):
        return "materialized_view"

    @property
    def models(self):
        return "models"

    def test_materialized_view_base(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run", "--select", "+mv"])

        self.assertTablesEqual("mv", "expected1")

        # Materialized View is not updated automatically.
        self.run_dbt(["run", "--select", "base"])
        self.assertTablesEqual("mv", "expected1")

        # Materialized View is updated after refresh.
        self.run_dbt(["run", "--select", "mv"])
        self.assertTablesEqual("mv", "expected2")

        # Materialized View is recreated with full refresh.
        self.run_dbt(["run", "--full-refresh", "--select", "mv"])
        self.assertTablesEqual("mv", "expected2")

    def test_materialized_view_no_cdf(self):
        # The base table is not CDF.
        result = self.run_dbt(["run", "--select", "+mv_nocdf"], expect_pass=False)
        assert len(result.results) == 2
        res: RunResult = result.results[1]
        assert res.status == RunStatus.Error
        assert (
            res.message
            and "The input for materialized view must have Change Data Feed enabled." in res.message
        )

    def test_materialized_view_based_on_view(self):
        result = self.run_dbt(["run", "--select", "+mv_on_view"], expect_pass=False)
        assert len(result.results) == 3
        res: RunResult = result.results[2]
        assert res.status == RunStatus.Error
        assert (
            res.message
            and " The input for materialized view cannot be based on views." in res.message
        )

    @use_profile("databricks_uc_sql_endpoint")
    def test_materialized_view_base_databricks_uc_sql_endpoint(self):
        self.test_materialized_view_base()

    @pytest.mark.skip(reason="not yet ready for production")
    @use_profile("databricks_uc_sql_endpoint")
    def test_materialized_view_no_cdf_databricks_uc_sql_endpoint(self):
        self.test_materialized_view_no_cdf()

    @pytest.mark.skip(reason="not yet ready for production")
    @use_profile("databricks_uc_sql_endpoint")
    def test_materialized_view_based_on_view_databricks_uc_sql_endpoint(self):
        self.test_materialized_view_based_on_view()
