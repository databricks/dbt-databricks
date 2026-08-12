from importlib import metadata

import pytest
from dbt.tests import util
from packaging import version

from tests.functional.adapter.microbatch import fixtures

try:
    from dbt.tests.util import patch_microbatch_end_time
except ImportError:
    from freezegun import freeze_time as patch_microbatch_end_time

dbt_version = metadata.version("dbt-core")


@pytest.mark.skipif(
    version.parse(dbt_version) < version.parse("1.9.0b1"),
    reason="Microbatch is not supported with this version of core",
)
@pytest.mark.skip_profile("databricks_cluster")
class TestConcurrentMicrobatchConfigChanges:
    """With concurrent batches enabled, per-model config changes (CLUSTER BY, SET TBLPROPERTIES)
    must run on the first batch only. If they ran per batch they would collide with the other
    batches' concurrent writes and fail them, dropping rows. See issue #1443."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_concurrent_microbatch": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "concurrent_input_model.sql": fixtures.concurrent_input_model_sql,
            "concurrent_microbatch_model.sql": fixtures.concurrent_microbatch_model_sql,
        }

    def test_all_batches_succeed_with_config_changes(self, project):
        # Initial backfill creates the table; the relation does not exist yet, so dbt-core runs
        # batches sequentially regardless of the flag.
        with patch_microbatch_end_time("2020-01-05 13:57:00"):
            util.run_dbt(["run"])

        # Re-run against the existing relation: dbt-core now runs the middle batches in parallel.
        # Every batch must land its row despite the per-model CLUSTER BY / SET TBLPROPERTIES.
        with patch_microbatch_end_time("2020-01-05 13:57:00"):
            util.run_dbt(["run", "--select", "concurrent_microbatch_model"])

        rows = project.run_sql(
            "select count(*) from "
            f"{project.database}.{project.test_schema}.concurrent_microbatch_model",
            fetch="all",
        )
        assert rows[0][0] == 5

        properties = project.run_sql(
            "show tblproperties "
            f"{project.database}.{project.test_schema}.concurrent_microbatch_model",
            fetch="all",
        )
        prop = {row[0]: row[1] for row in properties}
        assert prop.get("delta.columnMapping.mode") == "name"
        assert "id" in prop.get("clusteringColumns", "")
