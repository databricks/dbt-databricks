import os
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic import files
import pytest
from tests.functional.adapter.basic import fixtures


class TestIncrementalDelta(BaseIncremental):
    pass


class TestIncrementalDeltaNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalParquet(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_parquet"}

    @pytest.fixture(scope="class")
    def models(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "incremental.sql": fixtures.incremental_sql.replace(
                "'delta'", f"'parquet', location_root='{location_root}/parquet'"
            ),
            "schema.yml": files.schema_base_yml,
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalParquetHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": fixtures.incremental_sql.replace("'delta'", "'parquet'"),
            "schema.yml": files.schema_base_yml,
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalCSV(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_csv"}

    @pytest.fixture(scope="class")
    def models(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "incremental.sql": fixtures.incremental_sql.replace(
                "'delta'", f"'csv', location_root='{location_root}/csv'"
            ),
            "schema.yml": files.schema_base_yml,
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestIncrementalCSVHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": fixtures.incremental_sql.replace("'delta'", "'csv'"),
            "schema.yml": files.schema_base_yml,
        }
