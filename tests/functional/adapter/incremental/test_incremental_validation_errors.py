"""Incremental-strategy validation errors raised by validate.sql / strategies.sql.

Each fires as a compiler error during the run; the proof is the asserted run failure.
"""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


class TestMergeOnNonDeltaFormatRaises:
    """merge on a non-delta/hudi file_format -> dbt_databricks_validate_get_incremental_strategy
    raises before any DDL (validate.sql, the merge branch)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+unique_key": "id",
                "+file_format": "parquet",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_non_delta.sql": fixtures.base_model}

    def test_merge_on_parquet_raises(self, project):
        util.run_dbt(["run"], expect_pass=False)


class TestDeleteInsertWithoutUniqueKeyRaises:
    """delete+insert without unique_key -> validate.sql raises the missing-unique_key error
    on the first run (file_format stays delta so only the unique_key branch fires)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"delete_insert_no_key.sql": fixtures.base_model}

    def test_delete_insert_without_unique_key_raises(self, project):
        util.run_dbt(["run"], expect_pass=False)


class TestMergeUpdateAndExcludeColumnsRaises(RerunSafeMixin):
    """merge with BOTH merge_update_columns and merge_exclude_columns ->
    databricks__get_merge_update_columns (strategies.sql) raises. The raise lives on
    the merge build path, so it fires on the second (incremental) run."""

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("merge_update_exclude",)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+unique_key": "id",
                "+merge_update_columns": ["msg"],
                "+merge_exclude_columns": ["msg"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_update_exclude.sql": fixtures.base_model}

    def test_both_merge_column_configs_raise(self, project):
        # First run creates the relation (no merge yet) and must succeed.
        util.run_dbt(["run"])
        # Second run takes the merge path and the mutual-exclusion guard fires.
        util.run_dbt(["run"], expect_pass=False)
