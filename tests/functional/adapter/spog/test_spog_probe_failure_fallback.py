"""SPOG functional test: probe failure falls back to legacy and the run still succeeds.

Patches `probe_host` directly so we don't touch `requests.get` globally —
the SDK uses the shared `requests` module too, so a global stub would
also blow up auth.
"""

import os
from unittest import mock

import pytest

from tests.profiles import databricks_uc_sql_endpoint_target

pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_DATABRICKS_HOST_NAME"),
    reason="DBT_DATABRICKS_HOST_NAME not set; skipping",
)


class TestSpogProbeFailureFallback:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return databricks_uc_sql_endpoint_target()

    def test_probe_failure_run_succeeds(self, project):
        from dbt.tests.util import run_dbt

        from dbt.adapters.databricks.spog import probe
        from dbt.adapters.databricks.spog.probe import HostMetadata

        # Simulate exhausted-probe outcome by returning the same fallback
        # value the real retry loop returns when all attempts fail.
        probe.probe_host.cache_clear()
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.probe_host",
            return_value=HostMetadata(host_type=None),
        ):
            run_dbt(["debug"], expect_pass=True)
