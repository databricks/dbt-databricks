"""SPOG functional test: dbt debug surfaces SPOG status.

Skipped when the configured `http_path` doesn't include `?o=<workspace-id>`
(i.e. the env vars aren't pointing at a SPOG-enabled target).
"""

import os

import pytest

from dbt.adapters.databricks.spog.extract import extract_workspace_id

pytestmark = pytest.mark.skipif(
    extract_workspace_id(os.getenv("DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH")) is None,
    reason="http_path has no ?o= — not a SPOG target; skipping SPOG functional tests",
)


class TestSpogDebugOutput:
    def test_dbt_debug_reports_spog(self, project, capsys):
        from dbt.tests.util import run_dbt

        run_dbt(["debug"], expect_pass=True)
        captured = capsys.readouterr()
        output = captured.out + captured.err
        workspace_id = extract_workspace_id(os.environ["DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH"])
        assert "SPOG host" in output
        assert "workspace_id" in output
        assert workspace_id in output
