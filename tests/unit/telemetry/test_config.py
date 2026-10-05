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
        "connection_parameters, server_flag, expected",
        [
            pytest.param({"enable_dbt_telemetry": True}, "true", True, id="client_and_server_on"),
            pytest.param({"enable_dbt_telemetry": True}, "false", False, id="server_off"),
            pytest.param({}, "true", False, id="client_off"),
            pytest.param(
                {"force_enable_dbt_telemetry": True}, "false", True, id="force_ignores_server"
            ),
            pytest.param(
                {"enable_dbt_telemetry": True},
                RuntimeError("fetch failed"),
                False,
                id="fetch_failure_off",
            ),
        ],
    )
    def test_enabled_for_connection(
        self, monkeypatch, connection_parameters, server_flag, expected
    ):
        context = Mock()
        if isinstance(server_flag, Exception):
            context.get_flag_value.side_effect = server_flag
        else:
            context.get_flag_value.return_value = server_flag
        monkeypatch.setattr(
            config.FeatureFlagsContextFactory, "get_instance", Mock(return_value=context)
        )

        assert config.is_enabled_for_connection(_creds(connection_parameters), Mock()) is expected


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
