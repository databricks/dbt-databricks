from types import SimpleNamespace

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
