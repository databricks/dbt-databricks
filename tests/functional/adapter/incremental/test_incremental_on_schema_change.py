import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)


class BaseIncrementalOnSchemaChangeDatabricksSetup(BaseIncrementalOnSchemaChangeSetup):
    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        compare_source = "incremental_sync_all_columns"  # noqa: F841
        compare_target = "incremental_sync_all_columns_target"  # noqa: F841
        run_dbt(["run", "--models", select, "--full-refresh"])
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        compare_source = "incremental_sync_remove_only"  # noqa: F841
        compare_target = "incremental_sync_remove_only_target"  # noqa: F841
        run_dbt(["run", "--models", select, "--full-refresh"])
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message


class BaseIncrementalOnSchemaChangeDatabricks(
    BaseIncrementalOnSchemaChange, BaseIncrementalOnSchemaChangeDatabricksSetup
):
    def test_run_incremental_append_new_columns(self, project):
        # only adding new columns in supported
        self.run_incremental_append_new_columns(project)
        # handling columns that have been removed doesn"t work on Delta Lake today
        # self.run_incremental_append_new_columns_remove_one(project)


class TestAppendOnSchemaChangeDatabricks(BaseIncrementalOnSchemaChangeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }


@pytest.mark.skip_profile(
    "databricks_uc_cluster", "databricks_sql_endpoint", "databricks_uc_sql_endpoint"
)
class TestAppendParquetOnSchemaChangeDatabricks(BaseIncrementalOnSchemaChangeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            }
        }


@pytest.mark.skip_profile(
    "databricks_uc_cluster", "databricks_sql_endpoint", "databricks_uc_sql_endpoint"
)
class TestInsertOverwriteOnSchemaChangeDatabricks(BaseIncrementalOnSchemaChangeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+partition_by": "id",
                "+incremental_strategy": "insert_overwrite",
            }
        }


class TestMergeOnSchemaChangeDatabricks(BaseIncrementalOnSchemaChangeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+unique_key": "id",
            }
        }
