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
    def test_returns_true_for_supported_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_returns_true_for_higher_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.3.0",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_returns_false_for_lower_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.1.5",
        ):
            assert capabilities.connector_supports_spog() is False

    def test_handles_alpha_release(self):
        # 4.2.6a1 < 4.2.6 per PEP 440 ordering
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6a1",
        ):
            assert capabilities.connector_supports_spog() is False

    def test_handles_post_release(self):
        # 4.2.6.post1 > 4.2.6 per PEP 440 ordering
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6.post1",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_missing_package_returns_false(self):
        from importlib.metadata import PackageNotFoundError

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            side_effect=PackageNotFoundError("databricks-sql-connector"),
        ):
            assert capabilities.connector_supports_spog() is False


class TestSdkSupportsWorkspaceId:
    """The SDK's Config uses **kwargs + ConfigAttribute descriptors, so
    inspect.signature(Config) does not list workspace_id even on SPOG-capable
    versions. We feature-detect via hasattr on the class. Tests mock Config
    with a class that has (or lacks) workspace_id at the class level — the
    realistic shape, not an explicit constructor parameter."""

    def test_returns_true_when_config_has_workspace_id_attribute(self):
        # Class-level attribute, mirroring how databricks-sdk >= 0.103
        # declares workspace_id via ConfigAttribute().
        class FakeConfigWithWorkspaceId:
            workspace_id = None

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities.Config", FakeConfigWithWorkspaceId
        ):
            assert capabilities.sdk_supports_workspace_id() is True

    def test_returns_false_when_config_lacks_workspace_id_attribute(self):
        # Pre-SPOG SDK shape: no workspace_id at class level.
        class FakeConfigNoWorkspaceId:
            host = None

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities.Config", FakeConfigNoWorkspaceId
        ):
            assert capabilities.sdk_supports_workspace_id() is False

    def test_real_sdk_matches_feature_detection(self):
        """Integration check: against the actually-installed SDK, the
        predicate agrees with the real class shape. Would have caught the
        original inspect.signature bug — feature detection that disagrees
        with the real class is a real-world failure mode."""
        from databricks.sdk.core import Config as RealConfig

        capabilities.sdk_supports_workspace_id.cache_clear()
        expected = hasattr(RealConfig, "workspace_id")
        assert capabilities.sdk_supports_workspace_id() is expected
