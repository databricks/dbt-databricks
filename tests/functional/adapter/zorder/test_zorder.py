import pytest

from dbt.tests import util
from tests.functional.adapter.zorder import fixtures


class TestZOrder:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "zorder.sql": fixtures.zorder_sql,
        }

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_zorder(self, project):
        _, logs = util.run_dbt_and_capture(["--debug", "run"])
        assert "optimize" in logs
