import pytest

from dbt.tests import util
from dbt.tests.util import run_dbt, get_manifest
from tests.functional.adapter.metric_views.fixtures import (
    source_table,
    basic_metric_view,
)


# Test fixture for metric view without view_update_via_alter
metric_view_without_alter = """
{{
  config(
    materialized='metric_view',
    databricks_tags={
      'team': 'analytics',
      'environment': 'test'
    }
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
  - name: total_revenue
    expr: sum(revenue)
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViewSimpleChanges:
    """Test basic metric view behavior without configuration change detection"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "simple_metrics.sql": metric_view_without_alter,
        }

    def test_metric_view_always_recreates(self, project):
        """Test that metric view recreates without view_update_via_alter"""
        # First run creates the metric view
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(result.status == "success" for result in results)

        # Second run should recreate the metric view (full refresh behavior)
        results = run_dbt(["run", "--models", "simple_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the metric view still works
        metric_view_name = f"{project.database}.{project.test_schema}.simple_metrics"
        query_result = project.run_sql(f"""
            SELECT
                status,
                MEASURE(total_orders) as order_count,
                MEASURE(total_revenue) as revenue
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """, fetch="all")

        assert len(query_result) == 2
        status_data = {row[0]: (row[1], row[2]) for row in query_result}
        assert status_data['completed'] == (2, 250)
        assert status_data['pending'] == (1, 200)