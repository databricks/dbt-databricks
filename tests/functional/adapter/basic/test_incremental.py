import os

import pytest

from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_incremental import BaseIncrementalNotSchemaChange


class TestIncrementalDelta(BaseIncremental):
    pass


class TestIncrementalDeltaCapitalization(BaseIncremental):
    # Every test has a unique schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        test_file = request.__module__
        # We only want the last part of the name
        unique_schema = f"{prefix}_{test_file}".capitalize()
        return unique_schema


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
