from typing import Any, List
from mock import Mock
import pytest
from agate import Row
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshProcessor,
    ManualRefreshConfig,
    ScheduledRefreshConfig,
)
from dbt.exceptions import DbtRuntimeError
from agate import Table


class TestRefreshProcessor:
    @pytest.fixture
    def rows(self) -> List[List[str | Any]]:
        return [
            ["col_name", "data_type", "comment"],
            ["col_a", "int", "This is a comment"],
            [None, None, None],
            ["# Detailed Table Information", None, None],
            ["Catalog:", "default", None],
            ["Schema:", "default", None],
            ["Table:", "table_abc", None],
        ]

    def test_from_results__valid_schedule(self, rows):
        results = {
            "describe_extended": Table(
                rows=rows + [["Refresh Schedule", "CRON '*/5 * * * *' AT TIME ZONE 'UTC'"]]
            )
        }
        spec = RefreshProcessor.from_results(results)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")

    def test_from_results__manual(self, rows):
        results = {"describe_extended": Table(rows=rows + [["Refresh Schedule", "MANUAL"]])}
        spec = RefreshProcessor.from_results(results)
        assert spec == ManualRefreshConfig()

    def test_from_results__invalid(self, rows):
        results = {
            "describe_extended": Table(rows=rows + [["Refresh Schedule", "invalid description"]])
        }
        with pytest.raises(
            DbtRuntimeError,
            match="Could not parse schedule from description: invalid description",
        ):
            RefreshProcessor.from_results(results)

    def test_from_model_node__without_schedule(self):
        model = Mock()
        model.config.extra = {}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ManualRefreshConfig()

    def test_from_model_node__without_cron(self):
        model = Mock()
        model.config.extra = {"schedule": {"time_zone_value": "UTC"}}
        with pytest.raises(
            DbtRuntimeError,
            match="Schedule config must contain a 'cron' key, got {'time_zone_value': 'UTC'}.",
        ):
            RefreshProcessor.from_model_node(model)

    def test_from_model_node__without_timezone(self):
        model = Mock()
        model.config.extra = {"schedule": {"cron": "*/5 * * * *"}}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)

    def test_process_model_node__both(self):
        model = Mock()
        model.config.extra = {"schedule": {"cron": "*/5 * * * *", "time_zone_value": "UTC"}}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")


class TestScheduledRefreshConfig:
    def test_to_sql_clause__no_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *'"

    def test_to_sql_clause__with_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *' AT TIME ZONE 'UTC'"

    def test_to_alter_sql_clauses__alter_false(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC", alter=False)
        assert spec.to_alter_sql_clauses() == [
            "ADD SCHEDULE CRON '*/5 * * * *' AT TIME ZONE 'UTC'",
        ]

    def test_to_alter_sql_clauses__alter_true(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC", alter=True)
        assert spec.to_alter_sql_clauses() == [
            "ALTER SCHEDULE CRON '*/5 * * * *' AT TIME ZONE 'UTC'",
        ]

    def test_get_diff__other_not_refresh(self):
        config = ScheduledRefreshConfig(cron="*/5 * * * *")
        other = Mock()
        with pytest.raises(
            DbtRuntimeError,
            match="Cannot diff ScheduledRefreshConfig with Mock",
        ):
            config.get_diff(other)

    def test_get_diff__other_manual_refresh(self):
        config = ScheduledRefreshConfig(cron="*/5 * * * *")
        other = ManualRefreshConfig()
        diff = config.get_diff(other)
        assert diff == ScheduledRefreshConfig(cron="*/5 * * * *", alter=False)

    def test_get_diff__other_scheduled_refresh(self):
        config = ScheduledRefreshConfig(cron="*/5 * * * *")
        other = ScheduledRefreshConfig(cron="0 * * * *")
        diff = config.get_diff(other)
        assert diff == ScheduledRefreshConfig(cron="*/5 * * * *", alter=True)


class TestManualRefreshConfig:
    def test_get_diff__other_not_refresh(self):
        config = ManualRefreshConfig()
        other = Mock()
        with pytest.raises(
            DbtRuntimeError,
            match="Cannot diff ManualRefreshConfig with Mock",
        ):
            config.get_diff(other)

    def test_get_diff__other_scheduled_refresh(self):
        config = ManualRefreshConfig()
        other = ScheduledRefreshConfig(cron="*/5 * * * *")
        diff = config.get_diff(other)
        assert diff == config

    def test_get_diff__other_manual_refresh(self):
        config = ManualRefreshConfig()
        other = ManualRefreshConfig()
        diff = config.get_diff(other)
        assert diff == config

    def test_to_alter_sql_clauses(self):
        spec = ManualRefreshConfig()
        assert spec.to_alter_sql_clauses() == ["DROP SCHEDULE"]
