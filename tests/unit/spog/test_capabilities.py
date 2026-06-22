from importlib.metadata import PackageNotFoundError
from unittest import mock

import pytest

from dbt.adapters.databricks.spog import capabilities


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear @cache decorators between tests so each test sees a fresh state."""
    capabilities.connector_supports_spog.cache_clear()
    capabilities.sdk_supports_workspace_id.cache_clear()
    yield
    capabilities.connector_supports_spog.cache_clear()
    capabilities.sdk_supports_workspace_id.cache_clear()


class TestConnectorSupportsSpog:
    @pytest.mark.parametrize(
        ("connector_version", "expected"),
        [
            ("4.2.5", False),
            ("4.2.6", True),
        ],
    )
    def test_checks_connector_version_floor(self, connector_version, expected):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value=connector_version,
        ):
            assert capabilities.connector_supports_spog() is expected

    def test_missing_package_returns_false(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            side_effect=PackageNotFoundError("databricks-sql-connector"),
        ):
            assert capabilities.connector_supports_spog() is False


class TestSdkSupportsWorkspaceId:
    @pytest.mark.parametrize(
        ("config_cls", "expected"),
        [
            (type("ConfigWithWorkspaceId", (), {"workspace_id": None}), True),
            (type("ConfigWithoutWorkspaceId", (), {"host": None}), False),
        ],
    )
    def test_checks_config_workspace_id_attribute(self, config_cls, expected):
        with mock.patch("dbt.adapters.databricks.spog.capabilities.Config", config_cls):
            assert capabilities.sdk_supports_workspace_id() is expected
