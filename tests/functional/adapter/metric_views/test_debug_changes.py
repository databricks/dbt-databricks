import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.metric_views.fixtures import (
    source_table,
)

# Simple metric view for debugging
debug_metric_view = """
{{
  config(
    materialized='metric_view',
    view_update_via_alter=true
  )
}}

version: 0.1
source: "{{ ref('source_orders') }}"
dimensions:
  - name: status
    expr: status
measures:
  - name: total_orders
    expr: count(1)
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestDebugMetricViewChanges:
    """Test basic metric view behavior for debugging"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "debug_metrics.sql": debug_metric_view,
        }

    def test_debug_metric_view_second_run(self, project):
        """Test metric view second run behavior"""
        # First run creates the metric view
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(result.status == "success" for result in results)

        # Second run should work (CREATE OR REPLACE behavior)
        results = run_dbt(["run", "--models", "debug_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"
