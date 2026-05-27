from dbt.tests.util import run_dbt


class TestSpogDebugOutput:
    def test_dbt_debug_reports_spog(self, project):
        run_dbt(["debug"], expect_pass=True)
