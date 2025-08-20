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

        # Mock is_cluster to return True by default (cluster environment)
        context["adapter"].is_cluster.return_value = True

        # Mock behavior flags
        mock_behavior = Mock()
        mock_behavior.use_insert_replace_on = True  # Default to True for behavior flag
        context["adapter"].behavior = mock_behavior

    def test_get_insert_overwrite_sql__legacy_dbr_version(self, template, context, config):
        """Test that DBR < 17.1 uses the legacy DPO INSERT OVERWRITE syntax"""
        # Negative return value means DBR < 17.1
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
        """Test that DBR >= 17.1 uses REPLACE ON syntax"""
        # Positive return value means DBR > 17.1
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

        # Verify it uses REPLACE ON syntax
        expected_sql = """insert into table target_table as t
        replace on (t.partition_col <=> s.partition_col)
        (select a, b from source_table) as s"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__modern_dbr_version_multiple_partitions(
        self, template, context, config
    ):
        """Test that DBR >= 17.1 uses REPLACE ON syntax with multiple partition columns"""
        # Positive return value means DBR > 17.1
        context["adapter"].compare_dbr_version.return_value = 1
        config["partition_by"] = ["country", "name"]

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

        # Verify it uses REPLACE ON syntax with multiple conditions
        expected_sql = """insert into table target_table as t
        replace on (t.country <=> s.country AND t.name <=> s.name)
        (select a, b from source_table) as s"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__modern_dbr_version_only_clustering(
        self, template, context, config
    ):
        """
        Test that DBR >= 17.1 uses REPLACE ON syntax with liquid clustering columns when no
        partitioning is defined
        """
        # Positive return value means DBR > 17.1
        context["adapter"].compare_dbr_version.return_value = 1
        config["liquid_clustered_by"] = ["msg"]

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

        # Verify it uses REPLACE ON syntax with clustering columns only
        expected_sql = """insert into table target_table as t
        replace on (t.msg <=> s.msg)
        (select a, b from source_table) as s"""

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

    def test_get_insert_overwrite_sql__behavior_flag_disabled_cluster(
        self, template, context, config
    ):
        """Test that behavior flag disabled on cluster still uses REPLACE ON"""
        # Positive return value means DBR >= 17.1
        context["adapter"].compare_dbr_version.return_value = 1
        # Cluster environment (default from setup)
        context["adapter"].is_cluster.return_value = True
        # Disable the behavior flag
        context["adapter"].behavior.use_insert_replace_on = False
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

        # Verify it uses REPLACE ON syntax because cluster with DBR 17.1+ always uses new syntax
        expected_sql = """insert into table target_table as t
        replace on (t.partition_col <=> s.partition_col)
        (select a, b from source_table) as s"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__behavior_flag_disabled_old_dbr_cluster(
        self, template, context, config
    ):
        """Test that behavior flag disabled on old DBR cluster uses traditional INSERT OVERWRITE"""
        # Negative return value means DBR < 17.1
        context["adapter"].compare_dbr_version.return_value = -1
        # Cluster environment
        context["adapter"].is_cluster.return_value = True
        # Disable the behavior flag
        context["adapter"].behavior.use_insert_replace_on = False
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

        # Verify it uses traditional INSERT OVERWRITE syntax for old DBR cluster
        expected_sql = """insert overwrite table target_table
        partition (partition_col)
        select a, b from source_table"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__behavior_flag_disabled_sql_warehouse(
        self, template, context, config
    ):
        """Test that behavior flag disabled on SQL warehouse uses traditional INSERT OVERWRITE"""
        # Positive return value means DBR >= 17.1
        context["adapter"].compare_dbr_version.return_value = 1
        # SQL warehouse (not cluster)
        context["adapter"].is_cluster.return_value = False
        # Disable the behavior flag
        context["adapter"].behavior.use_replace_on_for_insert_overwrite = False
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

        # Verify it uses traditional INSERT OVERWRITE syntax for SQL warehouse without behavior flag
        expected_sql = """insert overwrite table target_table
        partition (partition_col)
        select a, b from source_table"""

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__behavior_flag_enabled_sql_warehouse(
        self, template, context, config
    ):
        """Test that behavior flag enabled on SQL warehouse uses REPLACE ON syntax"""
        # Positive return value means DBR >= 17.1
        context["adapter"].compare_dbr_version.return_value = 1
        # SQL warehouse (not cluster)
        context["adapter"].is_cluster.return_value = False
        # Enable the behavior flag
        context["adapter"].behavior.use_insert_replace_on = True
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

        # Verify it uses REPLACE ON syntax for SQL warehouse with behavior flag enabled
        expected_sql = """insert into table target_table as t
        replace on (t.partition_col <=> s.partition_col)
        (select a, b from source_table) as s"""

        self.assert_sql_equal(result, expected_sql)
