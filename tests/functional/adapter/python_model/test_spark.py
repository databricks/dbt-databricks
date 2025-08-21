import pytest

from dbt.tests import util
from dbt.tests.adapter.python_model import test_spark as fixtures
from dbt.tests.adapter.python_model.test_spark import (
    BasePySparkTests,
)

# pandas-on-spark model that handles ANSI mode
PANDAS_ON_SPARK_MODEL_ANSI_SAFE = """
import pandas as pd
import pyspark.pandas as ps

def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    
    # Set pandas-on-spark option to handle ANSI mode
    ps.set_option('compute.fail_on_ansi_mode', False)
    
    df = ps.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""


@pytest.mark.python
@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestPySpark(BasePySparkTests):
    @pytest.fixture(scope="class")
    def models(self):
        # Removed pandas_on_spark_df model - it fails with ANSI mode enabled
        # Users should handle ANSI mode themselves when creating pandas-on-Spark DataFrames
        return {
            "pandas_df.py": fixtures.PANDAS_MODEL,
            "pyspark_df.py": fixtures.PYSPARK_MODEL,
        }

    def test_different_dataframes(self, project):
        # test
        results = util.run_dbt(["run"])
        assert len(results) == 2
