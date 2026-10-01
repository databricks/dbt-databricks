from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.telemetry import config


def _creds(connection_parameters, **overrides):
    values = dict(
        connection_parameters=connection_parameters,
        auth_type=None,
        token=None,
        client_secret=None,
        azure_client_secret=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class TestOptIn:
    def test_defaults_off(self):
        assert config.is_enabled(_creds({})) is False
        assert config.is_enabled(_creds(None)) is False

    def test_explicit_opt_in(self):
        assert config.is_enabled(_creds({"enable_dbt_telemetry": True})) is True


class TestServerGate:
    @pytest.mark.parametrize(
        "connection_parameters, expected",
        [
            pytest.param({}, False, id="default_disabled"),
            pytest.param(
                {"enable_dbt_telemetry": True},
                True,
                id="client_enabled",
            ),
            pytest.param(
                {"enable_dbt_telemetry": False},
                False,
                id="client_disabled",
            ),
            pytest.param(
                {"enable_dbt_telemetry": False, "force_enable_dbt_telemetry": True},
                True,
                id="force_enabled",
            ),
        ],
    )
    def test_collection_eligibility(self, connection_parameters, expected):
        assert config.is_collection_enabled(_creds(connection_parameters)) is expected

    @pytest.mark.parametrize("flag_value, expected", [("true", True), ("false", False)])
    def test_server_flag_controls_dbt_telemetry(self, monkeypatch, flag_value, expected):
        context = Mock()
        context.get_flag_value.return_value = flag_value
        get_instance = Mock(return_value=context)
        monkeypatch.setattr(config.FeatureFlagsContextFactory, "get_instance", get_instance)
        connection = Mock()

        assert (
            config.is_enabled_for_connection(_creds({"enable_dbt_telemetry": True}), connection)
            is expected
        )
        get_instance.assert_called_once_with(connection)
        context.get_flag_value.assert_called_once_with(
            config.SERVER_ENABLE_FLAG, default_value=False
        )

    def test_force_enable_bypasses_server_flag(self, monkeypatch):
        get_instance = Mock()
        monkeypatch.setattr(config.FeatureFlagsContextFactory, "get_instance", get_instance)

        assert (
            config.is_enabled_for_connection(_creds({"force_enable_dbt_telemetry": True}), Mock())
            is True
        )
        get_instance.assert_not_called()

    def test_client_disabled_bypasses_server_flag(self, monkeypatch):
        get_instance = Mock()
        monkeypatch.setattr(config.FeatureFlagsContextFactory, "get_instance", get_instance)

        assert config.is_enabled_for_connection(_creds({}), Mock()) is False
        get_instance.assert_not_called()

    def test_server_flag_failure_defaults_off(self, monkeypatch):
        monkeypatch.setattr(
            config.FeatureFlagsContextFactory,
            "get_instance",
            Mock(side_effect=RuntimeError("feature flag fetch failed")),
        )

        assert (
            config.is_enabled_for_connection(_creds({"enable_dbt_telemetry": True}), Mock())
            is False
        )


class TestCommandEligibility:
    @pytest.mark.parametrize(
        "command, eligible",
        [
            ("build", True),
            ("run", True),
            ("test", True),
            ("seed", True),
            ("snapshot", True),
            ("compile", False),
            ("source freshness", False),
            ("run-operation", False),
            ("parse", False),
        ],
    )
    def test_command_eligibility(self, monkeypatch, command, eligible):
        from dbt import flags

        monkeypatch.setattr(flags, "get_flags", lambda: SimpleNamespace(WHICH=command))
        assert config.is_eligible_command() is eligible


class TestTransportEligibility:
    @pytest.mark.parametrize(
        "overrides, reusable",
        [
            pytest.param({"auth_type": "oauth"}, False, id="kernel_u2m"),
            pytest.param({"token": "token"}, True, id="kernel_pat"),
        ],
    )
    def test_kernel_transport(self, overrides, reusable):
        creds = _creds({"use_kernel": True}, **overrides)
        assert config.has_reusable_transport(creds) is reusable
