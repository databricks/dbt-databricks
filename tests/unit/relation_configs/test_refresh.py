from unittest.mock import Mock

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshConfig,
    RefreshMode,
    RefreshProcessor,
)
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
            match="Could not parse refresh schedule from describe extended: 'invalid description'",
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
            DbtRuntimeError, match=r"`time_zone_value` is only valid when `cron` is set"
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

    def test_eq__different_time_zone_not_equal(self):
        # Regression: same cron, different time zones must compare unequal.
        a = RefreshConfig(cron="*/5 * * * *", time_zone_value="America/Los_Angeles")
        b = RefreshConfig(cron="*/5 * * * *", time_zone_value="America/New_York")
        assert a != b

    def test_eq__same_time_zone_equal(self):
        a = RefreshConfig(cron="*/5 * * * *", time_zone_value="America/Los_Angeles")
        b = RefreshConfig(cron="*/5 * * * *", time_zone_value="America/Los_Angeles")
        assert a == b

    def test_eq__implicit_utc_equals_explicit_utc(self):
        a = RefreshConfig(cron="*/5 * * * *")
        b = RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        assert a == b


class TestRefreshConfigEquality:
    """Direct coverage of __eq__ across every mode and every normalization path."""

    def test_eq__manual_equals_manual(self):
        assert RefreshConfig() == RefreshConfig()

    def test_eq__manual_not_equal_cron(self):
        assert RefreshConfig() != RefreshConfig(cron="*/5 * * * *")

    def test_eq__manual_not_equal_every(self):
        assert RefreshConfig() != RefreshConfig(every="2 HOURS")

    def test_eq__manual_not_equal_on_update(self):
        assert RefreshConfig() != RefreshConfig(on_update=True)

    def test_eq__cross_mode_cron_vs_every(self):
        assert RefreshConfig(cron="*/5 * * * *") != RefreshConfig(every="2 HOURS")

    def test_eq__cross_mode_cron_vs_on_update(self):
        assert RefreshConfig(cron="*/5 * * * *") != RefreshConfig(on_update=True)

    def test_eq__cross_mode_every_vs_on_update(self):
        assert RefreshConfig(every="2 HOURS") != RefreshConfig(on_update=True)

    def test_eq__cron_case_insensitive_timezone(self):
        # Time-zone canonicalization is case-insensitive (server output is UPPERCASE).
        a = RefreshConfig(cron="*/5 * * * *", time_zone_value="utc")
        b = RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        assert a == b

    def test_eq__cron_no_tz_equals_etc_utc(self):
        # Empirically: a CRON created without a TZ comes back from DESCRIBE EXTENDED as
        # 'Etc/UTC'. Implicit-UTC desired must compare equal to the stored 'Etc/UTC'.
        desired = RefreshConfig(cron="*/5 * * * *")
        existing = RefreshConfig(cron="*/5 * * * *", time_zone_value="Etc/UTC")
        assert desired == existing
        assert hash(desired) == hash(existing)

    def test_eq__cron_utc_equals_etc_utc(self):
        a = RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        b = RefreshConfig(cron="*/5 * * * *", time_zone_value="Etc/UTC")
        assert a == b
        assert hash(a) == hash(b)

    def test_eq__cron_different_expr_not_equal(self):
        a = RefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        b = RefreshConfig(cron="0 * * * *", time_zone_value="UTC")
        assert a != b

    def test_eq__every_plural_vs_singular_normalized(self):
        assert RefreshConfig(every="1 DAY") == RefreshConfig(every="1 DAYS")

    def test_eq__every_case_insensitive(self):
        assert RefreshConfig(every="2 hours") == RefreshConfig(every="2 HOURS")

    def test_eq__every_different_count_not_equal(self):
        assert RefreshConfig(every="2 HOURS") != RefreshConfig(every="4 HOURS")

    def test_eq__every_different_unit_not_equal(self):
        assert RefreshConfig(every="2 HOURS") != RefreshConfig(every="2 DAYS")

    def test_eq__on_update_bare_equals_bare(self):
        assert RefreshConfig(on_update=True) == RefreshConfig(on_update=True)

    def test_eq__on_update_bare_not_equal_rate_limited(self):
        a = RefreshConfig(on_update=True)
        b = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        assert a != b
        # Symmetry: a != b ↔ b != a.
        assert b != a

    def test_eq__on_update_minutes_vs_seconds_normalized(self):
        a = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        b = RefreshConfig(on_update=True, at_most_every="900 SECOND")
        assert a == b

    def test_eq__on_update_hour_vs_seconds_normalized(self):
        a = RefreshConfig(on_update=True, at_most_every="1 HOUR")
        b = RefreshConfig(on_update=True, at_most_every="3600 SECOND")
        assert a == b

    def test_eq__on_update_different_intervals_not_equal(self):
        a = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        b = RefreshConfig(on_update=True, at_most_every="30 MINUTES")
        assert a != b

    def test_eq__non_refreshconfig_not_equal(self):
        # __eq__ must return False for foreign types, not raise.
        assert RefreshConfig() != object()
        assert RefreshConfig(cron="*/5 * * * *") != "*/5 * * * *"

    def test_eq__is_altered_does_not_affect_identity(self):
        # is_altered is a render-time hint, not part of identity.
        a = RefreshConfig(cron="*/5 * * * *", is_altered=False)
        b = RefreshConfig(cron="*/5 * * * *", is_altered=True)
        assert a == b


class TestRefreshModeDiscriminator:
    def test_mode__manual(self):
        assert RefreshConfig().mode == RefreshMode.MANUAL

    def test_mode__cron(self):
        assert RefreshConfig(cron="*/5 * * * *").mode == RefreshMode.CRON

    def test_mode__every(self):
        assert RefreshConfig(every="2 HOURS").mode == RefreshMode.EVERY

    def test_mode__on_update_bare(self):
        assert RefreshConfig(on_update=True).mode == RefreshMode.ON_UPDATE

    def test_mode__on_update_rate_limited(self):
        assert (
            RefreshConfig(on_update=True, at_most_every="15 MINUTES").mode == RefreshMode.ON_UPDATE
        )


class TestRefreshConfigValidation:
    def test_validation__multiple_modes_rejected(self):
        with pytest.raises(DbtRuntimeError, match="at most one"):
            RefreshConfig(cron="*/5 * * * *", every="2 HOURS")

    def test_validation__cron_with_on_update_rejected(self):
        with pytest.raises(DbtRuntimeError, match="at most one"):
            RefreshConfig(cron="*/5 * * * *", on_update=True)

    def test_validation__time_zone_without_cron_rejected(self):
        with pytest.raises(DbtRuntimeError, match="time_zone_value"):
            RefreshConfig(time_zone_value="UTC", every="2 HOURS")

    def test_validation__at_most_every_below_minimum_rejected(self):
        with pytest.raises(DbtRuntimeError, match="at least 60 seconds"):
            RefreshConfig(on_update=True, at_most_every="30 SECONDS")

    def test_validation__at_most_every_59_seconds_rejected(self):
        with pytest.raises(DbtRuntimeError, match="at least 60 seconds"):
            RefreshConfig(on_update=True, at_most_every="59 SECONDS")

    def test_validation__at_most_every_minimum_seconds_ok(self):
        c = RefreshConfig(on_update=True, at_most_every="60 SECONDS")
        assert c.mode == RefreshMode.ON_UPDATE

    def test_validation__at_most_every_minimum_minutes_ok(self):
        c = RefreshConfig(on_update=True, at_most_every="1 MINUTE")
        assert c.mode == RefreshMode.ON_UPDATE

    def test_validation__at_most_every_without_on_update_rejected(self):
        # Parallel to time_zone_value-without-cron: at_most_every is an option of on_update
        # and cannot stand alone.
        with pytest.raises(DbtRuntimeError, match="`at_most_every` is only valid"):
            RefreshConfig(at_most_every="15 MINUTES")


class TestRefreshProcessorNewShapes:
    def test_from_results__every_2_hours(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "EVERY 2 HOURS"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec == RefreshConfig(every="2 HOURS")

    def test_from_results__every_1_days_normalizes_to_1_day(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "EVERY 1 DAYS"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec.mode == RefreshMode.EVERY
        assert spec == RefreshConfig(every="1 DAY")

    def test_from_results__every_8_weeks(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "EVERY 8 WEEKS"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec == RefreshConfig(every="8 WEEKS")

    def test_from_results__trigger_bare(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", "TRIGGER ON UPDATE"]]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec.mode == RefreshMode.ON_UPDATE
        assert spec.at_most_every is None
        assert spec == RefreshConfig(on_update=True)

    def test_from_results__trigger_with_interval_seconds(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[
                    ["Refresh Schedule", "TRIGGER ON UPDATE AT MOST EVERY INTERVAL 900 SECOND"]
                ]
            )
        }
        spec = RefreshProcessor.from_relation_results(results)
        assert spec.mode == RefreshMode.ON_UPDATE
        assert spec.on_update is True
        # Server-stored '900 SECOND' must compare equal to user-input '15 MINUTES'.
        assert spec == RefreshConfig(on_update=True, at_most_every="15 MINUTES")

    def test_from_results__trigger_with_interval_does_not_raise(self):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[
                    ["Refresh Schedule", "TRIGGER ON UPDATE AT MOST EVERY INTERVAL 900 SECOND"]
                ]
            )
        }
        RefreshProcessor.from_relation_results(results)

    @pytest.mark.parametrize(
        "server_value,user_input,expected_seconds",
        [
            # Server normalizes every TRIGGER interval to integer SECOND, regardless of input unit.
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 60 SECOND", "1 MINUTE", 60),
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 300 SECOND", "5 MINUTES", 300),
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 3600 SECOND", "1 HOUR", 3600),
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 3600 SECOND", "60 MINUTES", 3600),
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 7200 SECOND", "2 HOURS", 7200),
            ("TRIGGER ON UPDATE AT MOST EVERY INTERVAL 86400 SECOND", "1 DAY", 86400),
        ],
    )
    def test_from_results__trigger_seconds_normalization_matches_user_input(
        self, server_value, user_input, expected_seconds
    ):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", server_value]]
            )
        }
        parsed = RefreshProcessor.from_relation_results(results)
        assert parsed.mode == RefreshMode.ON_UPDATE
        assert parsed.at_most_every == f"{expected_seconds} SECOND"
        # The user's input must round-trip to no-diff with the server-stored value.
        assert parsed == RefreshConfig(on_update=True, at_most_every=user_input)

    @pytest.mark.parametrize(
        "server_value,user_every",
        [
            # Server emits plural unit and preserves the integer. Input singular vs plural in dbt
            # config must round-trip equal to the stored plural form.
            ("EVERY 1 HOURS", "1 HOUR"),
            ("EVERY 2 HOURS", "2 HOURS"),
            ("EVERY 1 DAYS", "1 DAY"),
            ("EVERY 7 DAYS", "7 DAYS"),
            ("EVERY 1 WEEKS", "1 WEEK"),
            ("EVERY 8 WEEKS", "8 WEEKS"),
        ],
    )
    def test_from_results__every_plural_round_trip(self, server_value, user_every):
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[["Refresh Schedule", server_value]]
            )
        }
        parsed = RefreshProcessor.from_relation_results(results)
        assert parsed.mode == RefreshMode.EVERY
        assert parsed == RefreshConfig(every=user_every)

    def test_from_results__cron_etc_utc_round_trips_implicit_utc(self):
        # Empirical: SCHEDULE CRON '...' (no time zone) is stored as 'Etc/UTC' in the server.
        # A dbt config without time_zone_value must round-trip with no diff.
        results = {
            "describe_extended": fixtures.gen_describe_extended(
                detailed_table_info=[
                    ["Refresh Schedule", "CRON '0 0 * * * ? *' AT TIME ZONE 'Etc/UTC'"]
                ]
            )
        }
        parsed = RefreshProcessor.from_relation_results(results)
        assert parsed.mode == RefreshMode.CRON
        assert parsed.time_zone_value == "Etc/UTC"
        assert parsed == RefreshConfig(cron="0 0 * * * ? *")
        # No-diff check protects against the historical no-op ALTER on every dbt run.
        assert RefreshConfig(cron="0 0 * * * ? *").get_diff(parsed) is None


class TestFromRelationConfigNewShapes:
    def test_from_relation_config__every(self):
        model = Mock()
        model.config.extra = {"schedule": {"every": "2 HOURS"}}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig(every="2 HOURS")

    def test_from_relation_config__on_update_bool(self):
        model = Mock()
        model.config.extra = {"schedule": {"on_update": True}}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig(on_update=True)

    def test_from_relation_config__on_update_with_at_most_every(self):
        model = Mock()
        model.config.extra = {"schedule": {"on_update": True, "at_most_every": "15 MINUTES"}}
        spec = RefreshProcessor.from_relation_config(model)
        assert spec == RefreshConfig(on_update=True, at_most_every="15 MINUTES")

    def test_from_relation_config__at_most_every_without_on_update_rejected(self):
        model = Mock()
        model.config.extra = {"schedule": {"at_most_every": "15 MINUTES"}}
        with pytest.raises(DbtRuntimeError, match="`at_most_every` is only valid"):
            RefreshProcessor.from_relation_config(model)

    def test_from_relation_config__schedule_with_cron_and_every_rejected(self):
        model = Mock()
        model.config.extra = {
            "schedule": {"cron": "*/5 * * * *", "every": "2 HOURS"},
        }
        with pytest.raises(DbtRuntimeError, match="at most one"):
            RefreshProcessor.from_relation_config(model)

    def test_from_relation_config__schedule_with_cron_and_on_update_rejected(self):
        model = Mock()
        model.config.extra = {
            "schedule": {"cron": "*/5 * * * *", "on_update": True},
        }
        with pytest.raises(DbtRuntimeError, match="at most one"):
            RefreshProcessor.from_relation_config(model)


class TestRefreshConfigDiffNormalization:
    def test_diff__every_1_day_vs_1_days_no_diff(self):
        desired = RefreshConfig(every="1 DAY")
        existing = RefreshConfig(every="1 DAYS")
        assert desired.get_diff(existing) is None

    def test_diff__at_most_every_15_minutes_vs_900_second_no_diff(self):
        desired = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        existing = RefreshConfig(on_update=True, at_most_every="900 SECOND")
        assert desired.get_diff(existing) is None

    def test_diff__at_most_every_1_hour_vs_3600_second_no_diff(self):
        desired = RefreshConfig(on_update=True, at_most_every="1 HOUR")
        existing = RefreshConfig(on_update=True, at_most_every="3600 SECOND")
        assert desired.get_diff(existing) is None

    def test_diff__cross_mode_cron_to_every_alter(self):
        desired = RefreshConfig(every="2 HOURS")
        existing = RefreshConfig(cron="*/5 * * * *")
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is True

    def test_diff__cross_mode_every_to_on_update_alter(self):
        desired = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        existing = RefreshConfig(every="2 HOURS")
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is True

    def test_diff__bare_trigger_vs_rate_limited_alter(self):
        desired = RefreshConfig(on_update=True, at_most_every="15 MINUTES")
        existing = RefreshConfig(on_update=True)
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is True

    def test_diff__different_every_alter(self):
        desired = RefreshConfig(every="4 HOURS")
        existing = RefreshConfig(every="2 HOURS")
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is True

    def test_diff__manual_to_every_add(self):
        desired = RefreshConfig(every="2 HOURS")
        existing = RefreshConfig()
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is False

    def test_diff__every_to_manual_drop(self):
        desired = RefreshConfig()
        existing = RefreshConfig(every="2 HOURS")
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is False

    def test_diff__manual_to_on_update_add(self):
        desired = RefreshConfig(on_update=True)
        existing = RefreshConfig()
        diff = desired.get_diff(existing)
        assert diff is not None
        assert diff.is_altered is False
