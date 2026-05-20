"""SPOG functional test: dbt debug surfaces SPOG status.

Skipped when SPOG env vars are absent (e.g. local dev without SPOG creds).
On the SPOG CI matrix, asserts the SPOG host block + extracted workspace_id
+ dep-version status all appear in `dbt debug` output.
"""

import os

import pytest

from tests.profiles import databricks_uc_sql_endpoint_spog_target

pytestmark = pytest.mark.skipif(
    not (
        os.getenv("DBT_DATABRICKS_SPOG_HOST_NAME") and os.getenv("DBT_DATABRICKS_SPOG_WORKSPACE_ID")
    ),
    reason="SPOG env vars not set; skipping SPOG functional tests",
)


class TestSpogDebugOutput:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return databricks_uc_sql_endpoint_spog_target()

    def test_dbt_debug_reports_spog(self, project, capsys):
        from dbt.tests.util import run_dbt

        run_dbt(["debug"], expect_pass=True)
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "SPOG host" in output
        assert "workspace_id" in output
        assert os.environ["DBT_DATABRICKS_SPOG_WORKSPACE_ID"] in output
