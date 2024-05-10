import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_spark as fixtures
from dbt.tests.adapter.python_model.test_spark import (
    BasePySparkTests,
)


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPySpark(BasePySparkTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pandas_df.py": fixtures.PANDAS_MODEL,
            "pyspark_df.py": fixtures.PYSPARK_MODEL,
            "pandas_on_spark_df.py": fixtures.PANDAS_ON_SPARK_MODEL,
        }

    def test_different_dataframes(self, project):
        # test
        results = util.run_dbt(["run"])
        assert len(results) == 3
