from mock import Mock
import pytest
from agate import Row
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshProcessor,
    ManualRefreshConfig,
    ScheduledRefreshConfig,
)
from dbt.exceptions import DbtRuntimeError


class TestRefreshProcessor:
    def test_process_description_row_impl__valid_schedule(self):
        spec = RefreshProcessor.process_description_row_impl(
            Row(["Refresh Schedule", "CRON '*/5 * * * *' AT TIME ZONE 'UTC'"])
        )
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")

    def test_process_description_row_impl__manual(self):
        spec = RefreshProcessor.process_description_row_impl(Row(["Refresh Schedule", "MANUAL"]))
        assert spec == ManualRefreshConfig()

    def test_process_description_row_impl__invalid(self):
        with pytest.raises(
            DbtRuntimeError,
            match="Could not parse schedule from description: invalid description.",
        ):
            RefreshProcessor.process_description_row_impl(
                Row(["Refresh Schedule", "invalid description"])
            )

    def test_process_model_node__without_schedule(self):
        model = Mock()
        model.config.extra.get.return_value = {}
        spec = RefreshProcessor.process_model_node(model)
        assert spec == ManualRefreshConfig()

    def test_process_model_node__without_cron(self):
        model = Mock()
        model.config.extra.get.return_value = {"time_zone_value": "UTC"}
        with pytest.raises(
            DbtRuntimeError,
            match="Schedule config must contain a 'cron' key, got {'time_zone_value': 'UTC'}.",
        ):
            RefreshProcessor.process_model_node(model)

    def test_process_model_node__without_timezone(self):
        model = Mock()
        model.config.extra.get.return_value = {"cron": "*/5 * * * *"}
        spec = RefreshProcessor.process_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)

    def test_process_model_node__both(self):
        model = Mock()
        model.config.extra.get.return_value = {"cron": "*/5 * * * *", "time_zone_value": "UTC"}
        spec = RefreshProcessor.process_model_node(model)
        assert spec == ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")


class TestScheduledRefreshConfig:
    def test_to_schedule_clause__no_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value=None)
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *'"

    def test_to_schedule_clause__with_timezone(self):
        spec = ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        assert spec.to_sql_clause() == "SCHEDULE CRON '*/5 * * * *' AT TIME ZONE 'UTC'"
