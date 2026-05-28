from unittest import mock

import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.spog import fixtures


class TestSpogConnectionOpen:
    @pytest.fixture(scope="class")
    def models(self):
        return {"spog_smoke_model.sql": fixtures.spog_smoke_model}

    def test_spog_run_opens_connection(self, project):
        run_dbt(["run", "--select", "spog_smoke_model"], expect_pass=True)


class TestSpogProbeFailureFallback:
    @pytest.fixture(scope="class")
    def models(self):
        return {"spog_smoke_model.sql": fixtures.spog_smoke_model}

    def test_probe_failure_run_succeeds(self, project):
        from dbt.adapters.databricks.spog import probe
        from dbt.adapters.databricks.spog.probe import HostMetadata

        probe.probe_host.cache_clear()
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.probe_host",
            return_value=HostMetadata(host_type=None),
        ):
            run_dbt(["run", "--select", "spog_smoke_model"], expect_pass=True)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestSpogNamedCompute:
    @pytest.fixture(scope="class")
    def models(self):
        return {"spog_named_compute_model.sql": fixtures.spog_named_compute_model}

    def test_spog_named_compute_same_workspace_succeeds(self, project):
        run_dbt(["run", "--select", "spog_named_compute_model"], expect_pass=True)
