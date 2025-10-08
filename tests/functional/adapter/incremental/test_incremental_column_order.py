"""
Test that column order changes don't cause data corruption in incremental models.

This reproduces the bug where on_schema_change: sync_all_columns adds columns to the target
table, but INSERT statements use positional matching instead of name matching, causing values
to be inserted into the wrong columns.
"""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import MaterializationV2Mixin


# Model that adds a column in the middle on second run
incremental_reorder_sql = """
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='id'
) }}

{% if not is_incremental() %}
    -- First run: id, name, status
    select 1 as id, 'Alice' as name, 'active' as status
{% else %}
    -- Second run: adds 'score' column in middle position
    -- If using positional matching, 'Alice Updated' would go into score column (wrong!)
    select 1 as id, 100 as score, 'Alice Updated' as name, 'active' as status
    union all
    select 2 as id, 200 as score, 'Bob' as name, 'inactive' as status
{% endif %}
"""


class TestIncrementalColumnOrderBase:
    """Base test class for column order bug across all strategies"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_reorder.sql": incremental_reorder_sql,
        }

    def test_column_reorder_preserves_data_integrity(self, project):
        """
        Test that when columns are added in different order,
        values are inserted into correct columns (not positional corruption)
        """
        # First run: creates table with id, name, status
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Verify initial data
        actual = project.run_sql(
            "select id, name, status from incremental_reorder order by id", fetch="all"
        )
        assert len(actual) == 1
        assert actual[0] == (1, "Alice", "active")

        # Second run: adds score column and should update correctly
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Verify data is in correct columns (not corrupted by positional matching)
        # The bug would cause 'Alice Updated' to be in score column instead of name
        actual = project.run_sql(
            "select id, score, name, status from incremental_reorder order by id",
            fetch="all",
        )

        assert len(actual) == 2
        # Check that values are in the RIGHT columns (by name, not position)
        assert actual[0] == (1, 100, "Alice Updated", "active")
        assert actual[1] == (2, 200, "Bob", "inactive")


class TestIncrementalColumnOrderMerge(TestIncrementalColumnOrderBase):
    """Test column order with default merge strategy"""

    pass


class TestIncrementalColumnOrderMergeV2(
    MaterializationV2Mixin, TestIncrementalColumnOrderBase
):
    """Test column order with default merge strategy (V2 materialization)"""

    pass


class TestIncrementalColumnOrderAppend(TestIncrementalColumnOrderBase):
    """Test column order with append strategy"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "append"}}


class TestIncrementalColumnOrderAppendV2(
    MaterializationV2Mixin, TestIncrementalColumnOrderBase
):
    """Test column order with append strategy (V2 materialization)"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "append"}}


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestIncrementalColumnOrderInsertOverwrite(TestIncrementalColumnOrderBase):
    """Test column order with insert_overwrite strategy (not supported on SQL Warehouse)"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "insert_overwrite",
                "+partition_by": "status",
            }
        }


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestIncrementalColumnOrderInsertOverwriteV2(
    MaterializationV2Mixin, TestIncrementalColumnOrderBase
):
    """Test column order with insert_overwrite strategy (V2 materialization)"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "insert_overwrite",
                "+partition_by": "status",
            }
        }
