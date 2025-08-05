from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestInsertOverwriteMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    @pytest.fixture(scope="class")
    def spark_template_names(self) -> list:
        # Need spark templates for partition_cols macro
        return ["adapters.sql"]

    @pytest.fixture(autouse=True)
    def setup_mock_columns(self, context):
        """Mock the adapter methods needed for the macro"""
        # Mock get_columns_in_relation to return some test columns
        mock_column_a = Mock()
        mock_column_a.quoted = "a"
        mock_column_b = Mock()
        mock_column_b.quoted = "b"

        context["adapter"].get_columns_in_relation.return_value = [mock_column_a, mock_column_b]

    def test_get_insert_overwrite_sql__legacy_dbr_version(self, template, context, config):
        """Test that DBR < 16.3 uses traditional INSERT OVERWRITE syntax"""
        # Negative return value means DBR < 16.3
        context["adapter"].compare_dbr_version.return_value = -1
        config["partition_by"] = ["partition_col"]

        source_relation = Mock()
        source_relation.__str__ = lambda self: "source_table"
        target_relation = Mock()
        target_relation.__str__ = lambda self: "target_table"

        result = self.run_macro_raw(
            template,
            "get_insert_overwrite_sql",
            source_relation,
            target_relation,
        )

        # Verify it uses legacy INSERT OVERWRITE syntax
        expected_sql = """insert overwrite table target_table
        partition (partition_col)
        select a, b from source_table"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__modern_dbr_version(self, template, context, config):
        """Test that DBR >= 16.3 uses REPLACE USING syntax"""
        # Positive return value means DBR > 16.3
        context["adapter"].compare_dbr_version.return_value = 1
        config["partition_by"] = ["partition_col"]

        source_relation = Mock()
        source_relation.__str__ = lambda self: "source_table"
        target_relation = Mock()
        target_relation.__str__ = lambda self: "target_table"

        result = self.run_macro_raw(
            template,
            "get_insert_overwrite_sql",
            source_relation,
            target_relation,
        )

        # Verify it uses REPLACE USING syntax
        expected_sql = """insert into table target_table
        replace using (partition_col)
        select a, b from source_table"""

        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize("dbr_version_return", [-1, 0, 1])
    def test_get_insert_overwrite_sql__no_partitions(
        self, template, context, config, dbr_version_return
    ):
        """Test that empty partition_by falls back to INSERT OVERWRITE regardless of DBR version"""
        context["adapter"].compare_dbr_version.return_value = dbr_version_return
        # No partition_by set in config

        source_relation = Mock()
        source_relation.__str__ = lambda self: "source_table"
        target_relation = Mock()
        target_relation.__str__ = lambda self: "target_table"

        # Run the macro
        result = self.run_macro_raw(
            template,
            "get_insert_overwrite_sql",
            source_relation,
            target_relation,
        )

        # Verify it uses regular INSERT OVERWRITE syntax
        expected_sql = """insert overwrite table target_table select a, b from source_table"""

        self.assert_sql_equal(result, expected_sql)
