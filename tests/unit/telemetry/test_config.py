from types import SimpleNamespace

from dbt.adapters.databricks.telemetry import config


def _creds(connection_parameters):
    return SimpleNamespace(connection_parameters=connection_parameters)


class TestOptIn:
    def test_defaults_off(self):
        assert config.is_enabled(_creds({})) is False
        assert config.is_enabled(_creds(None)) is False

    def test_none_credentials_off(self):
        assert config.is_enabled(None) is False

    def test_explicit_opt_in(self):
        assert config.is_enabled(_creds({"enable_dbt_telemetry": True})) is True

    def test_falsey_value_off(self):
        assert config.is_enabled(_creds({"enable_dbt_telemetry": False})) is False
