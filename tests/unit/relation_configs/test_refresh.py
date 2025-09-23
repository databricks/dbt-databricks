from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig, RefreshProcessor
from dbt.exceptions import DbtRuntimeError
from tests.unit import fixtures


class TestRefreshProcessor:
    def test_from_results__valid_schedule(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "CRON '*/5 * * * *' AT TIME ZONE 'UTC'"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec == RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")

    def test_from_results__manual(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "MANUAL"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec == RefreshConfig()

    def test_from_results__invalid(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                [["Refresh Schedule", "invalid description"]]
            )
        }
        with pytest.raises(
            DbtRuntimeError,
            match="Could not parse schedule from description: invalid description",
        ):
            RefreshProcessor.from_relation_results(results)

    def test_from_model_node__without_schedule(self):
        model = Mock()
        model.config.extra = {}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig()

    def test_from_model_node__without_cron(self):
        model = Mock()
        model.config.extra = {"schedule": {"time_zone_value": "UTC"}}
        with pytest.raises(
            DbtRuntimeError,
            match="Schedule config must contain a 'cron' key, got {'time_zone_value': 'UTC'}",
        ):
            RefreshProcessor.from_relation_config(model)

    def test_from_model_node__without_timezone(self):
        model = Mock()
        model.config.extra = {"schedule": {"cron": "*/5 * * * *"}}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig(cron="*/5 * * * *", time_zone_value=None)

    def test_process_model_node__both(self):
        model = Mock()
        model.config.extra = {"schedule": {"cron": "*/5 * * * *", "time_zone_value": "UTC"}}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")


class TestRefreshConfig:
    def test_get_diff__scheduled_other_manual_refresh(self):
        config = RefreshConfig(cron="*/5 * * * *")
        other = RefreshConfig()
        diff = config.get_diff(other)
        assert diff == RefreshConfig(cron="*/5 * * * *", is_altered=False)

    def test_get_diff__scheduled_other_scheduled_refresh(self):
        config = RefreshConfig(cron="*/5 * * * *")
        other = RefreshConfig(cron="0 * * * *")
        diff = config.get_diff(other)
        assert diff == RefreshConfig(cron="*/5 * * * *", is_altered=True)

    def test_get_diff__manual_other_scheduled_refresh(self):
        config = RefreshConfig()
        other = RefreshConfig(cron="*/5 * * * *")
        diff = config.get_diff(other)
        assert diff == config

    def test_get_diff__manual_other_manual_refresh(self):
        config = RefreshConfig()
        other = RefreshConfig()
        diff = config.get_diff(other)
        assert diff is None
