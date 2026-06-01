from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestBuildSnapshotTable(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "snapshot_helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations"]

    def test_build_snapshot_table_columns_first(self, template, context):
        class Cols:
            dbt_scd_id = "dbt_scd_id"
            dbt_updated_at = "dbt_updated_at"
            dbt_valid_from = "dbt_valid_from"
            dbt_valid_to = "dbt_valid_to"
            dbt_is_deleted = "dbt_is_deleted"

        context["get_snapshot_table_column_names"] = lambda: Cols()
        context["get_dbt_valid_to_current"] = lambda s, c: "nullif('u', 'u') as dbt_valid_to"

        strategy = Mock()
        strategy.scd_id = "'some_scd_id'"
        strategy.updated_at = "'u'"
        strategy.hard_deletes = "ignore"

        sql = self.run_macro(
            template, "databricks__build_snapshot_table", strategy, "select 1 as a"
        )

        expected = """
        select
            'some_scd_id' as dbt_scd_id,
            'u' as dbt_updated_at,
            'u' as dbt_valid_from,
            nullif('u', 'u') as dbt_valid_to,
            *
        from (
            select 1 as a
        ) sbq
        """
        self.assert_sql_equal(expected, sql)

        # explicit check for column ordering
        scd_id_pos = sql.index("dbt_scd_id")
        wildcard_pos = sql.index("*")
        assert scd_id_pos < wildcard_pos, (
            "Meta-columns must appear before * in SELECT to ensure they fall "
            "within Databricks' first-32-column statistics window"
        )

    def test_build_snapshot_table_columns_first_new_record(self, template, context):
        class Cols:
            dbt_scd_id = "dbt_scd_id"
            dbt_updated_at = "dbt_updated_at"
            dbt_valid_from = "dbt_valid_from"
            dbt_valid_to = "dbt_valid_to"
            dbt_is_deleted = "dbt_is_deleted"

        context["get_snapshot_table_column_names"] = lambda: Cols()
        context["get_dbt_valid_to_current"] = lambda s, c: "nullif('u', 'u') as dbt_valid_to"

        strategy = Mock()
        strategy.scd_id = "'some_scd_id'"
        strategy.updated_at = "'u'"
        strategy.hard_deletes = "new_record"

        sql = self.run_macro(
            template, "databricks__build_snapshot_table", strategy, "select 1 as a"
        )

        expected = """
        select
            'some_scd_id' as dbt_scd_id,
            'u' as dbt_updated_at,
            'u' as dbt_valid_from,
            nullif('u', 'u') as dbt_valid_to,
            false as dbt_is_deleted,
            *
        from (
            select 1 as a
        ) sbq
        """
        self.assert_sql_equal(expected, sql)

        # explicit check for column ordering
        scd_id_pos = sql.index("dbt_scd_id")
        wildcard_pos = sql.index("*")
        assert scd_id_pos < wildcard_pos, (
            "Meta-columns must appear before * in SELECT to ensure they fall "
            "within Databricks' first-32-column statistics window"
        )

    def test_build_snapshot_table_custom_column_names(self, template, context):
        class Cols:
            dbt_scd_id = "_scd_id"
            dbt_updated_at = "_updated_at"
            dbt_valid_from = "start_date"
            dbt_valid_to = "end_date"
            dbt_is_deleted = "is_deleted"

        context["get_snapshot_table_column_names"] = lambda: Cols()
        context["get_dbt_valid_to_current"] = lambda s, c: "nullif('u', 'u') as end_date"

        strategy = Mock()
        strategy.scd_id = "'some_scd_id'"
        strategy.updated_at = "'u'"
        strategy.hard_deletes = "new_record"

        sql = self.run_macro(
            template, "databricks__build_snapshot_table", strategy, "select 1 as a"
        )

        expected = """
        select
            'some_scd_id' as _scd_id,
            'u' as _updated_at,
            'u' as start_date,
            nullif('u', 'u') as end_date,
            false as is_deleted,
            *
        from (
            select 1 as a
        ) sbq
        """
        self.assert_sql_equal(expected, sql)

        # check that original defaults aren't there
        assert "_scd_id" in sql
        assert "dbt_scd_id" not in sql

        scd_id_pos = sql.index("_scd_id")
        wildcard_pos = sql.index("*")
        assert scd_id_pos < wildcard_pos, "Meta-columns must precede wildcard"

    def test_build_snapshot_table_custom_column_names_ignore(self, template, context):
        class Cols:
            dbt_scd_id = "_scd_id"
            dbt_updated_at = "_updated_at"
            dbt_valid_from = "start_date"
            dbt_valid_to = "end_date"
            dbt_is_deleted = "is_deleted"

        context["get_snapshot_table_column_names"] = lambda: Cols()
        context["get_dbt_valid_to_current"] = lambda s, c: "nullif('u', 'u') as end_date"

        strategy = Mock()
        strategy.scd_id = "'some_scd_id'"
        strategy.updated_at = "'u'"
        strategy.hard_deletes = "ignore"

        sql = self.run_macro(
            template, "databricks__build_snapshot_table", strategy, "select 1 as a"
        )

        expected = """
        select
            'some_scd_id' as _scd_id,
            'u' as _updated_at,
            'u' as start_date,
            nullif('u', 'u') as end_date,
            *
        from (
            select 1 as a
        ) sbq
        """
        self.assert_sql_equal(expected, sql)

        # check that original defaults aren't there
        assert "_scd_id" in sql
        assert "dbt_scd_id" not in sql

        scd_id_pos = sql.index("_scd_id")
        wildcard_pos = sql.index("*")
        assert scd_id_pos < wildcard_pos, "Meta-columns must precede wildcard"
