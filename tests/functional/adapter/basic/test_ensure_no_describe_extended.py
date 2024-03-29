import pytest

from dbt.tests import util
from tests.functional.adapter.basic import fixtures


class TestEnsureNoDescribeExtended:
    """Tests in this class exist to ensure we don't call describe extended unnecessarily.
    This became a problem due to needing to discern tables from streaming tables, which is not
    relevant on hive, but users on hive were having all of their tables describe extended-ed.
    We only need to call describe extended if we are using a UC catalog and we can't determine the
    type of the materialization."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": fixtures.basic_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": fixtures.basic_model_sql}

    def test_ensure_no_describe_extended(self, project):
        # Add some existing data to ensure we don't try to 'describe extended' it.
        util.run_dbt(["seed"])

        _, log_output = util.run_dbt_and_capture(["run"])
        assert "describe extended" not in log_output
