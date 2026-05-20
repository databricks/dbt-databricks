"""SPOG functional test: probe failure falls back to legacy with a WARN.

Simulates a network failure on the /.well-known/databricks-config probe and
asserts:
  1. The WARN is logged at runtime (visible in dbt output).
  2. The run still succeeds against the legacy target (non-fatal fallback).
"""

import os
from unittest import mock

import pytest
import requests

from tests.profiles import databricks_uc_sql_endpoint_target

pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_DATABRICKS_HOST_NAME"),
    reason="Legacy env vars not set; skipping",
)


class TestSpogProbeFailureFallback:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        # Use the legacy target so the run can complete even when the
        # probe is forced to fail.
        return databricks_uc_sql_endpoint_target()

    def test_probe_failure_warn_and_run_succeeds(self, project):
        from dbt.tests.util import run_dbt

        from dbt.adapters.databricks.spog import probe

        # Clear the cache so this test's patch takes effect.
        probe.probe_host.cache_clear()
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                side_effect=requests.ConnectionError("simulated network failure"),
            ),
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep"),
            mock.patch("dbt.adapters.databricks.spog.probe.logger") as mock_logger,
        ):
            # Run must still succeed — probe failure is non-fatal.
            run_dbt(["debug"], expect_pass=True)

        # And the WARN must have been logged.
        warn_calls = mock_logger.warning.call_args_list
        assert warn_calls, "expected probe_host to log a WARN on exhaustion"
        warn_msg = warn_calls[0][0][0]
        assert "SPOG discovery probe" in warn_msg
