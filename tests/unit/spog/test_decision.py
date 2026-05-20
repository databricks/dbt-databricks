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
                    "dbt.adapters.databricks.spog.decision.probe_host",
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

    def test_spog_missing_o_raises(self, patch_decision):
        patch_decision(host_type="unified")
        with pytest.raises(DbtConfigError, match=r"http_path.*\?o="):
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc"],
            )

    def test_spog_sdk_too_old_raises(self, patch_decision):
        patch_decision(host_type="unified", sdk_ok=False)
        with pytest.raises(DbtConfigError, match=r"databricks-sdk.*>=\s*0\.104"):
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )

    def test_spog_connector_too_old_raises(self, patch_decision):
        patch_decision(host_type="unified", connector_ok=False)
        with pytest.raises(DbtConfigError, match=r"databricks-sql-connector.*>=\s*4\.2\.6"):
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )

    def test_spog_both_deps_too_old_raises_connector_error(self, patch_decision):
        """When BOTH connector and SDK lack SPOG support, the connector check
        runs first (deterministic ordering) so the error mentions the
        connector. Locks the order so a future refactor can't silently reorder."""
        patch_decision(host_type="unified", connector_ok=False, sdk_ok=False)
        with pytest.raises(DbtConfigError) as excinfo:
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )
        msg = str(excinfo.value)
        assert "databricks-sql-connector" in msg and "4.2.6" in msg
        assert "databricks-sdk" not in msg, (
            "Connector check must be raised first; SDK check should not run."
        )

    def test_non_spog_with_explicit_o_raises(self, patch_decision):
        patch_decision(host_type="workspace")
        with pytest.raises(DbtConfigError, match=r"not a SPOG"):
            decision.check_spog_preconditions(
                host="legacy.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )

    def test_non_spog_with_cluster_path_embed_is_benign(self, patch_decision):
        """Cluster paths embed the workspace id in /o/<id>/ as a natural URL
        property — that's not a user-written ?o= and works on any host.
        Common config: legacy host + a per-compute cluster alias whose path
        carries the workspace id implicitly. Must not raise."""
        patch_decision(host_type="workspace")
        ws_id = decision.check_spog_preconditions(
            host="legacy.azuredatabricks.net",
            http_paths=[
                "/sql/1.0/warehouses/default",  # no ?o=
                "/sql/protocolv1/o/6436897454825492/0605-cluster",  # /o/<id>/ embed
            ],
        )
        # workspace_id returned (extracted from cluster path), but no error
        assert ws_id == "6436897454825492"

    def test_non_spog_legacy_returns_none(self, patch_decision):
        patch_decision(host_type="workspace")
        assert (
            decision.check_spog_preconditions(
                host="legacy.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc"],
            )
            is None
        )

    def test_probe_failure_permissive_with_o(self, patch_decision):
        patch_decision(host_type=None)  # probe failed
        ws_id = decision.check_spog_preconditions(
            host="flaky.example.com",
            http_paths=["/sql/1.0/warehouses/abc?o=64"],
        )
        # Permissive: returns the extracted id, lets request proceed
        assert ws_id == "64"

    def test_probe_failure_permissive_without_o(self, patch_decision):
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
        patch_decision(host_type="unified")
        with pytest.raises(DbtConfigError, match=r"conflicting"):
            decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=[
                    "/sql/1.0/warehouses/a?o=64",
                    "/sql/1.0/warehouses/b?o=99",
                ],
            )
