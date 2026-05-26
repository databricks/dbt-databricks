"""SPOG functional test: dbt debug surfaces SPOG status.

Skipped when the configured `http_path` doesn't include `?o=<workspace-id>`
(i.e. the env vars aren't pointing at a SPOG-enabled target).
"""

import os

import pytest

from dbt.adapters.databricks.spog.extract import extract_workspace_id


def _resolved_http_path() -> str | None:
    # Cover all four env-var shapes the live workflows set per profile:
    # warehouse (UC_ENDPOINT or generic HTTP_PATH), UC cluster, plain cluster.
    return (
        os.getenv("DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH")
        or os.getenv("DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH")
        or os.getenv("DBT_DATABRICKS_CLUSTER_HTTP_PATH")
        or os.getenv("DBT_DATABRICKS_HTTP_PATH")
    )


pytestmark = pytest.mark.skipif(
    extract_workspace_id(_resolved_http_path()) is None,
    reason="http_path has no ?o= — not a SPOG target; skipping SPOG functional tests",
)


class TestSpogDebugOutput:
    def test_dbt_debug_reports_spog(self, project, capsys):
        from dbt.tests.util import run_dbt

        run_dbt(["debug"], expect_pass=True)
        captured = capsys.readouterr()
        output = captured.out + captured.err
        workspace_id = extract_workspace_id(_resolved_http_path())
        assert "SPOG host" in output
        assert "workspace_id" in output
        assert workspace_id in output
