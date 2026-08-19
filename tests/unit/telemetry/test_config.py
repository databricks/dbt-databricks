from types import SimpleNamespace

from dbt.adapters.databricks.telemetry import config


def _creds(connection_parameters):
    return SimpleNamespace(
        connection_parameters=connection_parameters,
        auth_type=None,
        token=None,
        client_secret=None,
        azure_client_secret=None,
    )


class TestOptIn:
    def test_defaults_off(self):
        assert config.is_enabled(_creds({})) is False
        assert config.is_enabled(_creds(None)) is False

    def test_explicit_opt_in(self):
        assert config.is_enabled(_creds({"enable_dbt_telemetry": True})) is True


class TestCommandEligibility:
    def test_warehouse_graph_commands_are_eligible(self, monkeypatch):
        from dbt import flags

        monkeypatch.setattr(flags, "get_flags", lambda: SimpleNamespace(WHICH="build"))
        assert config.is_eligible_command() is True

    def test_parse_only_and_unwired_task_shapes_are_ineligible(self, monkeypatch):
        from dbt import flags

        for command in ("compile", "source freshness", "run-operation"):
            monkeypatch.setattr(
                flags,
                "get_flags",
                lambda command=command: SimpleNamespace(WHICH=command),
            )
            assert config.is_eligible_command() is False


class TestTransportEligibility:
    def test_kernel_u2m_is_ineligible(self):
        creds = _creds({"use_kernel": True})
        creds.auth_type = "oauth"
        assert config.has_reusable_transport(creds) is False

    def test_kernel_pat_is_eligible(self):
        creds = _creds({"use_kernel": True})
        creds.token = "token"
        assert config.has_reusable_transport(creds) is True
