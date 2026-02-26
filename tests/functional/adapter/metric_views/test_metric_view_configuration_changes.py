import pytest
from dbt.tests import util
from dbt.tests.util import run_dbt

from tests.functional.adapter.metric_views.fixtures import (
    source_table,
)

# Test fixture for metric view with tags configuration
metric_view_with_tags = """
{{
  config(
    materialized='metric_view',
    view_update_via_alter=true,
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

# Updated tag configuration for testing ALTER
metric_view_with_updated_tags = """
{{
  config(
    materialized='metric_view',
    view_update_via_alter=true,
    databricks_tags={
      'team': 'data-engineering',
      'environment': 'production',
      'owner': 'dbt-team'
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

# Changed YAML definition that requires CREATE OR REPLACE
metric_view_with_changed_definition = """
{{
  config(
    materialized='metric_view',
    view_update_via_alter=true,
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
  - name: order_date
    expr: order_date
measures:
  - name: total_orders
    expr: count(1)
  - name: total_revenue
    expr: sum(revenue)
  - name: avg_revenue
    expr: avg(revenue)
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViewConfigurationChanges:
    """Test metric view configuration change handling"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "config_change_metrics.sql": metric_view_with_tags,
        }

    def test_metric_view_tag_only_changes_via_alter(self, project):
        """Test that tag-only changes use ALTER instead of CREATE OR REPLACE"""
        # First run creates the metric view
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(result.status == "success" for result in results)

        # Update the model with different tags
        util.write_file(metric_view_with_updated_tags, "models", "config_change_metrics.sql")

        # Second run should use ALTER for tags
        results = run_dbt(["run", "--models", "config_change_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the metric view still works
        metric_view_name = f"{project.database}.{project.test_schema}.config_change_metrics"
        query_result = project.run_sql(
            f"""
            SELECT
                status,
                MEASURE(total_orders) as order_count,
                MEASURE(total_revenue) as revenue
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """,
            fetch="all",
        )

        assert len(query_result) == 2
        status_data = {row[0]: (row[1], row[2]) for row in query_result}
        assert status_data["completed"] == (2, 250)
        assert status_data["pending"] == (1, 200)

    def test_metric_view_definition_changes_require_replace(self, project):
        """Test that YAML definition changes use CREATE OR REPLACE"""
        # First run creates the metric view
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(result.status == "success" for result in results)

        # Update the model with changed YAML definition
        util.write_file(metric_view_with_changed_definition, "models", "config_change_metrics.sql")

        # Second run should use CREATE OR REPLACE for YAML changes
        results = run_dbt(["run", "--models", "config_change_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the updated metric view works with new measure
        metric_view_name = f"{project.database}.{project.test_schema}.config_change_metrics"
        query_result = project.run_sql(
            f"""
            SELECT
                status,
                MEASURE(total_orders) as order_count,
                MEASURE(total_revenue) as revenue,
                MEASURE(avg_revenue) as avg_revenue
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """,
            fetch="all",
        )

        assert len(query_result) == 2
        status_data = {row[0]: (row[1], row[2], row[3]) for row in query_result}
        assert status_data["completed"] == (2, 250, 125.0)  # (100+150)/2 = 125
        assert status_data["pending"] == (1, 200, 200.0)

    def test_no_changes_skip_materialization(self, project):
        """Test that no changes result in no-op"""
        # First run creates the metric view
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(result.status == "success" for result in results)

        # Second run with no changes should be a no-op
        results = run_dbt(["run", "--models", "config_change_metrics"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the metric view still works
        metric_view_name = f"{project.database}.{project.test_schema}.config_change_metrics"
        query_result = project.run_sql(
            f"""
            SELECT
                status,
                MEASURE(total_orders) as order_count
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """,
            fetch="all",
        )

        assert len(query_result) == 2
        status_data = {row[0]: row[1] for row in query_result}
        assert status_data["completed"] == 2
        assert status_data["pending"] == 1
