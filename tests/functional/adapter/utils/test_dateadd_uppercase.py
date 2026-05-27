import pytest
from dbt.tests import util

# Exercises the parse-time stub for `has_dbr_capability` on SQL warehouses.
#
# `databricks__dateadd` branches on `has_dbr_capability('timestampdiff')`. If the
# parse stub returned False, the legacy `spark__dateadd` path is selected at compile
# time and raises a compile error on uppercase dateparts. With the warehouse stub
# returning True, the modern `timestampadd` path is selected and the run succeeds.
model_sql = """
select {{ dateadd('DAY', 1, "cast('2024-01-01' as timestamp)") }} as ts
"""


class TestDateAddUppercaseDatepartOnWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {"uppercase_dateadd.sql": model_sql}

    @pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
    def test_uppercase_datepart_compiles_and_runs(self, project):
        util.run_dbt(["run"])
