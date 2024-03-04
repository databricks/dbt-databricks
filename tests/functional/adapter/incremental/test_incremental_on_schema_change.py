from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
import pytest
from dbt.tests.adapter.incremental import fixtures
from tests.functional.adapter.incremental import fixtures as fixture_overrides


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": fixtures._MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": fixtures._MODELS__INCREMENTAL_IGNORE,
            "incremental_sync_remove_only_target.sql": fixtures._MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            "incremental_ignore_target.sql": fixtures._MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": fixtures._MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": fixture_overrides._MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": fixtures._MODELS__A,
            "incremental_append_new_columns_target.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            "incremental_append_new_columns.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_sync_all_columns_target.sql": fixture_overrides._MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_append_new_columns_remove_one_target.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }

    def test_run_incremental_sync_all_columns(self, project):
        # Anything other than additions to the target table will fail for now
        self.run_incremental_sync_all_columns(project)
