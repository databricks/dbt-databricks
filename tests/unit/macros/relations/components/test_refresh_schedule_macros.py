import pytest

from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from tests.unit.macros.base import MacroTestBase


class _RefreshMacrosBase(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "refresh_schedule.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]


class TestRefreshScheduleCreateMacro(_RefreshMacrosBase):
    def test_cron_with_time_zone(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(cron="0 0 * * * ? *", time_zone_value="UTC"),
        )
        self.assert_sql_equal(result, "schedule cron '0 0 * * * ? *' at time zone 'utc'")

    def test_cron_without_time_zone(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(cron="0 0 * * * ? *"),
        )
        self.assert_sql_equal(result, "schedule cron '0 0 * * * ? *'")

    def test_every(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(every="2 HOURS"),
        )
        self.assert_sql_equal(result, "schedule every 2 hours")

    def test_on_update_bare(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(on_update=True),
        )
        self.assert_sql_equal(result, "trigger on update")

    def test_on_update_with_at_most_every(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(on_update=True, at_most_every="15 MINUTES"),
        )
        self.assert_sql_equal(result, "trigger on update at most every interval 15 minutes")

    def test_manual_emits_nothing(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_create_sql_refresh_schedule",
            RefreshConfig(),
        )
        assert result.strip() == ""


class TestRefreshScheduleAlterMacro(_RefreshMacrosBase):
    def test_add_cron(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(cron="0 0 * * * ? *", time_zone_value="UTC", is_altered=False),
        )
        self.assert_sql_equal(result, "add schedule cron '0 0 * * * ? *' at time zone 'utc'")

    def test_alter_cron(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(cron="0 0 * * * ? *", time_zone_value="UTC", is_altered=True),
        )
        self.assert_sql_equal(result, "alter schedule cron '0 0 * * * ? *' at time zone 'utc'")

    def test_add_every(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(every="2 HOURS", is_altered=False),
        )
        self.assert_sql_equal(result, "add schedule every 2 hours")

    def test_alter_every(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(every="4 HOURS", is_altered=True),
        )
        self.assert_sql_equal(result, "alter schedule every 4 hours")

    def test_add_trigger_bare(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(on_update=True, is_altered=False),
        )
        self.assert_sql_equal(result, "add trigger on update")

    def test_alter_trigger_with_at_most_every(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(on_update=True, at_most_every="15 MINUTES", is_altered=True),
        )
        self.assert_sql_equal(result, "alter trigger on update at most every interval 15 minutes")

    def test_drop_schedule_when_no_mode_set(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "get_alter_sql_refresh_schedule",
            RefreshConfig(),
        )
        self.assert_sql_equal(result, "drop schedule")
