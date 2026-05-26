"""SPOG functional test: probe failure is non-fatal — dbt debug still succeeds.

Patches `probe_host` directly so we don't touch `requests.get` globally —
the SDK uses the shared `requests` module too, so a global stub would
also blow up auth.

Profile-agnostic: inherits the active --profile from the test invocation
so it runs identically on databricks_cluster, databricks_uc_cluster, and
databricks_uc_sql_endpoint shards.
"""

import os
from unittest import mock

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_DATABRICKS_HOST_NAME"),
    reason="DBT_DATABRICKS_HOST_NAME not set; skipping",
)


class TestSpogProbeFailureFallback:
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
