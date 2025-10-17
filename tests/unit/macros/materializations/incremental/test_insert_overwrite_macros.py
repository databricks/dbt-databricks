from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.dbr_capabilities import DBRCapability
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
        mock_column_a.name = "a"
        mock_column_a.quoted = "a"
        mock_column_b = Mock()
        mock_column_b.name = "b"
        mock_column_b.quoted = "b"

        context["adapter"].get_columns_in_relation.return_value = [mock_column_a, mock_column_b]

        # Mock is_cluster to return True by default (cluster environment)
        context["adapter"].is_cluster.return_value = True

        # Mock has_capability - by default support both replace_on and insert_by_name
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return True  # Default to DBR 17.1+
            elif capability_name == 'insert_by_name':
                return True  # Default to DBR 12.2+
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        # Mock behavior flags
        mock_behavior = Mock()
        mock_behavior.use_replace_on_for_insert_overwrite = (
            True  # Default to True for behavior flag
        )
        context["adapter"].behavior = mock_behavior

        # Mock exceptions.warn to return empty string to avoid polluting SQL output
        context["exceptions"].warn.return_value = ""

    @pytest.mark.parametrize(
        "has_replace_on_capability,expected_sql",
        [
            (
                False,  # DBR < 17.1
                """
                insert overwrite table target_table
                partition (a)
                by name
                select * from source_table
                """,
            ),
            (
                True,  # DBR >= 17.1
                """
                insert into table target_table as t
                replace on (t.a <=> s.a)
                (select a, b from source_table) as s
                """,
            ),
        ],
    )
    def test_get_dynamic_insert_overwrite_sql__dbr_version_syntax(
        self, template, context, config, has_replace_on_capability, expected_sql
    ):
        """Test that different DBR versions use appropriate dynamic insert overwrite syntax"""

        # Mock has_capability to control REPLACE ON support
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return has_replace_on_capability
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        config["partition_by"] = ["a"]

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

        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize(
        "config_key,test_description",
        [
            (
                "partition_by",
                "multiple partition columns",
            ),
            (
                "liquid_clustered_by",
                "liquid clustering columns when no partitioning is defined",
            ),
        ],
    )
    def test_get_insert_overwrite_sql__modern_dbr_version_multiple_columns(
        self, template, context, config, config_key, test_description
    ):
        """Test that DBR >= 17.1 uses REPLACE ON syntax with multiple columns"""

        # Mock has_capability to support REPLACE ON
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return True
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        config[config_key] = ["a", "b"]

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
        expected_sql = """
            insert into table target_table as t
            replace on (t.a <=> s.a AND t.b <=> s.b)
            (select a, b from source_table) as s
        """

        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize("has_replace_on", [False, True])
    def test_get_insert_overwrite_sql__no_partitions_and_liquid_clustered(
        self, template, context, config, has_replace_on
    ):
        """
        Test that empty partition_by and liquid_clustered_by falls back to INSERT OVERWRITE
        regardless of DBR version
        """

        # Mock has_capability
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return has_replace_on
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
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
        expected_sql = """
            insert overwrite table target_table by name select * from source_table
        """

        self.assert_sql_equal(result, expected_sql)

    def test_get_insert_overwrite_sql__behavior_flag_disabled_cluster(
        self, template, context, config
    ):
        """Test behavior flag disabled on cluster still uses REPLACE ON (ignored for clusters)"""

        # Mock has_capability to support REPLACE ON
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return True
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        # Cluster environment (default from setup)
        context["adapter"].is_cluster.return_value = True
        # Disable the behavior flag
        context["adapter"].behavior.use_replace_on_for_insert_overwrite = False
        config["partition_by"] = ["a"]

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
        expected_sql = """
            insert into table target_table as t
            replace on (t.a <=> s.a)
            (select a, b from source_table) as s
        """

        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize(
        "use_replace_on_flag",
        [
            False,  # Behavior flag disabled
            True,  # Behavior flag enabled
        ],
    )
    def test_get_insert_overwrite_sql__old_dbr_cluster_behavior_flag(
        self, template, context, config, use_replace_on_flag
    ):
        """Old DBR cluster always uses traditional INSERT OVERWRITE regardless of behavior flag"""

        # Mock has_capability to NOT support REPLACE ON (DBR < 17.1)
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return False
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        # Cluster environment
        context["adapter"].is_cluster.return_value = True
        # Set the behavior flag (should not affect old DBR clusters)
        context["adapter"].behavior.use_replace_on_for_insert_overwrite = use_replace_on_flag
        config["partition_by"] = ["a"]

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

        # Verify it always uses traditional INSERT OVERWRITE syntax for old DBR cluster
        expected_sql = """
            insert overwrite table target_table
            partition (a)
            by name
            select * from source_table
        """

        self.assert_sql_equal(result, expected_sql)

    @pytest.mark.parametrize(
        "use_replace_on_flag,expected_sql",
        [
            (
                False,  # Behavior flag disabled
                """
                insert overwrite table target_table
                partition (a)
                by name
                select * from source_table
                """,
            ),
            (
                True,  # Behavior flag enabled
                """
                insert into table target_table as t
                replace on (t.a <=> s.a)
                (select a, b from source_table) as s
                """,
            ),
        ],
    )
    def test_get_insert_overwrite_sql__sql_warehouse_behavior_flag(
        self, template, context, config, use_replace_on_flag, expected_sql
    ):
        """Test that SQL warehouse behavior flag controls INSERT OVERWRITE syntax"""

        # Mock has_capability (SQL warehouses are assumed capable)
        def has_dbr_capability_side_effect(capability_name):
            if capability_name == 'replace_on':
                return True  # SQL warehouses are assumed capable
            elif capability_name == 'insert_by_name':
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)
        # SQL warehouse (not cluster)
        context["adapter"].is_cluster.return_value = False
        # Set the behavior flag
        context["adapter"].behavior.use_replace_on_for_insert_overwrite = use_replace_on_flag
        config["partition_by"] = ["a"]

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

        self.assert_sql_equal(result, expected_sql)
