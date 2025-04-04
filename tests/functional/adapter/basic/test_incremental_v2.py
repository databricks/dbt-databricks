import os

import pytest

from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange


class BaseIncrementalV2(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental",
            "flags": {"use_materialization_v2": True},
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield


class TestIncrementalDelta(BaseIncrementalV2):
    pass


class TestIncrementalDeltaCapitalization(BaseIncrementalV2):
    # Every test has a unique schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        test_file = request.__module__
        # We only want the last part of the name
        unique_schema = f"{prefix}_{test_file}".capitalize()
        return unique_schema


class BaseIncrementalNotSchemaChangeV2(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental", "flags": {"use_materialization_v2": True}}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield


class TestIncrementalDeltaNotSchemaChange(BaseIncrementalNotSchemaChangeV2):
    pass


@pytest.mark.external
@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalParquet(BaseIncrementalV2):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+file_format": "parquet",
                "+location_root": f"{location_root}/parquet",
                "+include_full_name_in_path": "true",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalParquetHive(BaseIncrementalV2):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.external
@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalCSV(BaseIncrementalV2):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+file_format": "csv",
                "+location_root": f"{location_root}/csv",
                "+include_full_name_in_path": "true",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalCSVHive(BaseIncrementalV2):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+file_format": "csv",
                "+incremental_strategy": "append",
            },
        }
