import os
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange
import pytest


class TestIncrementalDelta(BaseIncremental):
    pass


class TestIncrementalDeltaNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalParquet(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "models": {
                "+file_format": "parquet",
                "+location_root": f"{location_root}/parquet",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalParquetHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalCSV(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "models": {
                "+file_format": "csv",
                "+location_root": f"{location_root}/csv",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalCSVHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "csv",
                "+incremental_strategy": "append",
            },
        }
