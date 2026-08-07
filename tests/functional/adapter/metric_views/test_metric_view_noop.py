"""An unchanged metric view with `view_update_via_alter` must be a no-op on re-run.

Databricks re-renders the stored metric-view definition (requoting `source`
double->single, rewriting flow-style lists to block style), so a text comparison of
the model YAML against the stored View Text always registered a change and dbt
re-issued `ALTER VIEW ... AS` on every run. The definition is compared structurally,
so an unchanged re-run leaves the view untouched.
"""

import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.metric_views.fixtures import (
    metric_view_with_synonyms,
    source_table,
)


def _last_altered(project, name):
    return project.run_sql(
        f"SELECT last_altered FROM {project.database}.information_schema.tables "
        f"WHERE table_schema = '{project.test_schema}' AND table_name = '{name}'",
        fetch="all",
    )[0][0]


@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViewNoopOnRerun:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+view_update_via_alter": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "noop_metrics.sql": metric_view_with_synonyms,
        }

    def test_unchanged_metric_view_is_noop(self, project):
        run_dbt(["run"])
        before = _last_altered(project, "noop_metrics")

        results = run_dbt(["run", "--models", "noop_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"

        after = _last_altered(project, "noop_metrics")
        assert after == before, (
            f"metric view was re-altered on an unchanged re-run: {before} -> {after}"
        )
