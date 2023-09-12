import os
import pytest
from tests.integration.base import DBTIntegrationTest, use_profile


class TestAvoidDescribeExtended(DBTIntegrationTest):
    """Tests in this class exist to ensure we don't call describe extended unnecessarily.
    This became a problem due to needing to discern tables from streaming tables, which is not
    relevant on hive, but users on hive were having all of their tables describe extended-ed.
    We only need to call describe extended if we are using a UC catalog and we can't determine the
    type of the materialization."""

    @property
    def schema(self):
        return "schema"

    @property
    def models(self):
        return "models"

    def _test_avoid_describe_extended(self):
        # Add some existing data to ensure we don't try to 'describe extended' it.
        self.run_dbt(["seed"])
        _, log_output = self.run_dbt_and_capture(["run"])
        self.assertNotIn("describe extended", log_output)

    @use_profile("databricks_cluster")
    def test_avoid_describe_extended_databricks_cluster(self):
        """When UC is not enabled, we can assumed that all tables are regular tables"""
        self._test_avoid_describe_extended()

    @use_profile("databricks_uc_sql_endpoint")
    def test_avoid_describe_extended_databricks_uc_sql_endpoint(self):
        """When UC is enabled, regular tables are marked as such"""
        self._test_avoid_describe_extended()
