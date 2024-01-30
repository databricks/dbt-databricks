import pytest
from tests.unit.macros.relation_configs.base import RelationConfigTestBase


class TestRefreshScheduleMacros(RelationConfigTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "refresh_schedule.sql"

    def test_get_create_sql_refresh_schedule__manual(self, template):
        s = template.get_create_sql_refresh_schedule(None, None)
        assert s == ""

    def test_get_create_sql_refresh_schedule__cron_only(self, template):
        s = template.get_create_sql_refresh_schedule("*/5 * * * * ?", None)
        assert s == "SCHEDULE CRON '*/5 * * * * ?'"

    def test_get_create_sql_refresh_schedule__with_time_zone(self, template):
        s = template.get_create_sql_refresh_schedule("*/5 * * * * ?", "UTC")
        assert s == "SCHEDULE CRON '*/5 * * * * ?' AT TIME ZONE 'UTC'"

    def test_get_alter_sql_refresh_schedule__manual(self, template):
        s = template.get_alter_sql_refresh_schedule(None, None, False)
        assert s == "DROP SCHEDULE"

    def test_get_alter_sql_refresh_schedule__add_cron_only(self, template):
        s = template.get_alter_sql_refresh_schedule("*/5 * * * * ?", None, False)
        assert s == "ADD SCHEDULE CRON '*/5 * * * * ?'"

    def test_get_alter_sql_refresh_schedule__add_with_time_zone(self, template):
        s = template.get_alter_sql_refresh_schedule("*/5 * * * * ?", "UTC", False)
        assert s == "ADD SCHEDULE CRON '*/5 * * * * ?' AT TIME ZONE 'UTC'"

    def test_get_alter_sql_refresh_schedule__alter_cron_only(self, template):
        s = template.get_alter_sql_refresh_schedule("*/5 * * * * ?", None, True)
        assert s == "ALTER SCHEDULE CRON '*/5 * * * * ?'"

    def test_get_alter_sql_refresh_schedule__alter_with_time_zone(self, template):
        s = template.get_alter_sql_refresh_schedule("*/5 * * * * ?", "UTC", True)
        assert s == "ALTER SCHEDULE CRON '*/5 * * * * ?' AT TIME ZONE 'UTC'"
