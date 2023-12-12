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
        model.config.extra.get.return_value = {}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ManualRefreshConfig()

    def test_from_model_node__without_cron(self):
        model = Mock()
        model.config.extra.get.return_value = {"time_zone_value": "UTC"}
        with pytest.raises(
            DbtRuntimeError,
            match="Schedule config must contain a 'cron' key, got {'time_zone_value': 'UTC'}.",
        ):
            RefreshProcessor.from_model_node(model)

    def test_from_model_node__without_timezone(self):
        model = Mock()
        model.config.extra.get.return_value = {"cron": "*/5 * * * *"}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)

    def test_process_model_node__both(self):
        model = Mock()
        model.config.extra.get.return_value = {"cron": "*/5 * * * *", "time_zone_value": "UTC"}
        spec = RefreshProcessor.from_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")


class TestScheduledRefreshConfig:
    def test_to_schedule_clause__no_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *'"

    def test_to_schedule_clause__with_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *' AT TIME ZONE 'UTC'"
