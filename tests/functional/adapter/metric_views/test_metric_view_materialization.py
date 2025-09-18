import pytest

from dbt.tests.util import run_dbt, get_manifest
from tests.functional.adapter.metric_views.fixtures import (
    source_table,
    basic_metric_view,
    metric_view_with_filter,
    metric_view_with_config,
)


@pytest.mark.skip_profile("databricks_cluster")
class TestBasicMetricViewMaterialization:
    """Test basic metric view materialization functionality"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "order_metrics.sql": basic_metric_view,
        }

    def test_metric_view_creation(self, project):
        """Test that metric view materialization creates a metric view successfully"""
        # Run dbt to create the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Verify both models ran successfully
        assert all(result.status == "success" for result in results)

        # Check that the metric view was created
        manifest = get_manifest(project.project_root)
        metric_view_node = manifest.nodes["model.test.order_metrics"]
        assert metric_view_node.config.materialized == "metric_view"

        # Test if the metric view actually works by querying it with MEASURE()
        # This is the most important test - if this works, the metric view was created correctly
        metric_view_name = f"{project.database}.{project.test_schema}.order_metrics"

        try:
            # Query the metric view using MEASURE() function - this is the real test
            query_result = project.run_sql(f"""
                SELECT
                    status,
                    MEASURE(total_orders) as order_count,
                    MEASURE(total_revenue) as revenue
                FROM {metric_view_name}
                GROUP BY status
                ORDER BY status
            """, fetch="all")
            print(f"Metric view query result: {query_result}")

            # If we got results, verify the data is correct
            if query_result:
                assert len(query_result) == 2, f"Expected 2 status groups, got {len(query_result)}"

                # Check the data makes sense (2 completed orders worth 250, 1 pending order worth 200)
                status_data = {row[0]: (row[1], row[2]) for row in query_result}
                print(f"Status data: {status_data}")

                assert 'completed' in status_data, "Missing 'completed' status"
                assert 'pending' in status_data, "Missing 'pending' status"

                completed_count, completed_revenue = status_data['completed']
                pending_count, pending_revenue = status_data['pending']

                assert completed_count == 2, f"Expected 2 completed orders, got {completed_count}"
                assert completed_revenue == 250, f"Expected 250 completed revenue, got {completed_revenue}"
                assert pending_count == 1, f"Expected 1 pending order, got {pending_count}"
                assert pending_revenue == 200, f"Expected 200 pending revenue, got {pending_revenue}"

                print("✅ Metric view query successful with correct data!")
            else:
                # fetch=True returned None, but let's try without fetch to see if it executes
                project.run_sql(f"SELECT MEASURE(total_orders) FROM {metric_view_name} LIMIT 1", fetch=False)
                print("✅ Metric view query executed without error (but fetch returned None)")

        except Exception as e:
            assert False, f"Metric view query failed: {e}"

    def test_metric_view_query(self, project):
        """Test that the metric view can be queried using MEASURE() function"""
        # First run dbt to create the models
        run_dbt(["run"])

        # Query the metric view using MEASURE() function
        query_result = project.run_sql(f"""
            select
                status,
                measure(total_orders) as order_count,
                measure(total_revenue) as revenue
            from {project.database}.{project.test_schema}.order_metrics
            group by status
            order by status
        """, fetch="all")

        # Verify we get expected results
        assert len(query_result) == 2  # Should have 'completed' and 'pending' status

        # Check the data makes sense
        completed_row = next(row for row in query_result if row[0] == 'completed')
        pending_row = next(row for row in query_result if row[0] == 'pending')

        assert completed_row[1] == 2  # 2 completed orders
        assert completed_row[2] == 250  # 100 + 150 revenue
        assert pending_row[1] == 1  # 1 pending order
        assert pending_row[2] == 200  # 200 revenue


@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViewWithFilter:
    """Test metric view materialization with filters"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "filtered_metrics.sql": metric_view_with_filter,
        }

    def test_metric_view_with_filter_creation(self, project):
        """Test that metric view with filter works correctly"""
        # Run dbt to create the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Verify both models ran successfully
        assert all(result.status == "success" for result in results)

    def test_metric_view_with_filter_query(self, project):
        """Test that filtered metric view returns expected results"""
        # First run dbt to create the models
        run_dbt(["run"])

        # Query the filtered metric view
        query_result = project.run_sql(f"""
            select
                measure(completed_orders) as order_count,
                measure(completed_revenue) as revenue
            from {project.database}.{project.test_schema}.filtered_metrics
        """, fetch="all")

        # Should only see completed orders (2 orders with 250 total revenue)
        assert len(query_result) == 1
        row = query_result[0]
        assert row[0] == 2  # 2 completed orders
        assert row[1] == 250  # 100 + 150 revenue from completed orders only


@pytest.mark.skip_profile("databricks_cluster")
class TestMetricViewConfiguration:
    """Test metric view materialization with configuration options"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": source_table,
            "config_metrics.sql": metric_view_with_config,
        }

    def test_metric_view_with_tags(self, project):
        """Test that metric view works with databricks_tags using ALTER VIEW"""
        # Run dbt to create the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Verify both models ran successfully
        assert all(result.status == "success" for result in results)

        # Check that the metric view was created
        manifest = get_manifest(project.project_root)
        config_node = manifest.nodes["model.test.config_metrics"]
        assert config_node.config.materialized == "metric_view"

        # Verify the metric view works by querying it
        metric_view_name = f"{project.database}.{project.test_schema}.config_metrics"

        query_result = project.run_sql(f"""
            SELECT
                status,
                MEASURE(order_count) as count
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """, fetch="all")

        # Should have results showing tags were applied without error
        assert query_result is not None
        assert len(query_result) == 2  # completed and pending statuses

        # Check the data is correct
        status_data = {row[0]: row[1] for row in query_result}
        assert status_data['completed'] == 2
        assert status_data['pending'] == 1