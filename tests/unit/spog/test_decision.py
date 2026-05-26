from contextlib import ExitStack
from unittest import mock

import pytest
from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.spog import decision
from dbt.adapters.databricks.spog.probe import HostMetadata


@pytest.fixture
def patch_decision():
    """Patch capability + probe modules to fixed responses for one test.

    Yields a function: patch_decision(host_type=..., connector_ok=True, sdk_ok=True).
    All patches auto-unwind at test teardown.
    """
    with ExitStack() as stack:

        def _apply(*, host_type, connector_ok=True, sdk_ok=True):
            stack.enter_context(
                mock.patch(
                    "dbt.adapters.databricks.spog.probe.probe_host",
                    return_value=HostMetadata(host_type=host_type),
                )
            )
            stack.enter_context(
                mock.patch(
                    "dbt.adapters.databricks.spog.decision.connector_supports_spog",
                    return_value=connector_ok,
                )
            )
            stack.enter_context(
                mock.patch(
                    "dbt.adapters.databricks.spog.decision.sdk_supports_workspace_id",
                    return_value=sdk_ok,
                )
            )

        yield _apply


class TestCheckSpogPreconditions:
    def test_spog_happy_path(self, patch_decision):
        patch_decision(host_type="unified")
        ws_id = decision.check_spog_preconditions(
            host="peco.azuredatabricks.net",
            http_paths=["/sql/1.0/warehouses/abc?o=64"],
        )
        assert ws_id == "64"

    def test_spog_missing_o_warns_and_returns_none(self, patch_decision):
        patch_decision(host_type="unified")
        with mock.patch("dbt.adapters.databricks.spog.decision.logger") as mock_logger:
            ws_id = decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc"],
            )
        assert ws_id is None
        mock_logger.warning.assert_called_once()

    def test_non_spog_with_explicit_o_warns_and_returns_id(self, patch_decision):
        patch_decision(host_type="workspace")
        with mock.patch("dbt.adapters.databricks.spog.decision.logger") as mock_logger:
            ws_id = decision.check_spog_preconditions(
                host="legacy.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )
        assert ws_id == "64"
        mock_logger.warning.assert_called_once()

    @pytest.mark.parametrize(
        ("connector_ok", "sdk_ok"),
        [(True, False), (False, True)],
        ids=["sdk-too-old", "connector-too-old"],
    )
    def test_pre_spog_deps_short_circuit_no_probe(self, connector_ok, sdk_ok):
        """When either dep is below the SPOG floor, return None without probing.
        SPOG can't activate on pre-SPOG deps anyway."""
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.decision.connector_supports_spog",
                return_value=connector_ok,
            ),
            mock.patch(
                "dbt.adapters.databricks.spog.decision.sdk_supports_workspace_id",
                return_value=sdk_ok,
            ),
            mock.patch("dbt.adapters.databricks.spog.probe.probe_host") as probe,
        ):
            result = decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )
        assert result is None
        probe.assert_not_called()

    def test_legacy_cluster_path_no_o_returns_none_no_warn(self, patch_decision):
        """Cluster path /o/<id>/ is *not* a SPOG declaration. On a legacy host
        with no ?o=, there's nothing for us to mirror and nothing to warn
        about — `?o=` is the only opt-in marker."""
        patch_decision(host_type="workspace")
        with mock.patch("dbt.adapters.databricks.spog.decision.logger") as mock_logger:
            ws_id = decision.check_spog_preconditions(
                host="legacy.azuredatabricks.net",
                http_paths=[
                    "/sql/1.0/warehouses/default",
                    "/sql/protocolv1/o/6436897454825492/0605-cluster",
                ],
            )
        assert ws_id is None
        mock_logger.warning.assert_not_called()

    def test_non_spog_legacy_returns_none(self, patch_decision):
        patch_decision(host_type="workspace")
        assert (
            decision.check_spog_preconditions(
                host="legacy.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc"],
            )
            is None
        )

    def test_probe_failure_returns_extracted_id(self, patch_decision):
        """Probe failure is non-fatal; surface whatever the user explicitly
        declared via ?o= and let downstream handle routing."""
        patch_decision(host_type=None)
        ws_id = decision.check_spog_preconditions(
            host="flaky.example.com",
            http_paths=["/sql/1.0/warehouses/abc?o=64"],
        )
        assert ws_id == "64"

    def test_probe_failure_without_o_returns_none(self, patch_decision):
        patch_decision(host_type=None)
        assert (
            decision.check_spog_preconditions(
                host="flaky.example.com",
                http_paths=["/sql/1.0/warehouses/abc"],
            )
            is None
        )

    def test_multi_compute_consistent_returns_id(self, patch_decision):
        patch_decision(host_type="unified")
        ws_id = decision.check_spog_preconditions(
            host="peco.azuredatabricks.net",
            http_paths=[
                "/sql/1.0/warehouses/a?o=64",
                "/sql/1.0/warehouses/b?o=64",
            ],
        )
        assert ws_id == "64"

    def test_multi_compute_conflicting_raises(self, patch_decision):
        """The only hard raise: different ?o= values on http_paths that
        share a connection. This is unambiguously broken regardless of
        host type, so we fail fast instead of warning."""
        patch_decision(host_type="unified")
        with pytest.raises(DbtConfigError, match=r"conflicting"):
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=[
                    "/sql/1.0/warehouses/a?o=64",
                    "/sql/1.0/warehouses/b?o=99",
                ],
            )
