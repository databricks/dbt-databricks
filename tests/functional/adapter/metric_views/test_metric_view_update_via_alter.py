import pytest
from dbt.tests import util

from tests.functional.adapter.metric_views import fixtures


class BaseUpdateMetricView:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_orders.sql": fixtures.source_table,
            "test_metric_view.sql": fixtures.metric_view_with_config,
        }


class BaseUpdateMetricViewQuery(BaseUpdateMetricView):
    def test_metric_view_update_query(self, project):
        """Test that metric view query can be updated via ALTER VIEW AS"""
        # First run creates the metric view
        util.run_dbt(["run"])

        # Update the query by changing the metric definition
        updated_metric_view = """
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
  - name: order_date
    expr: order_date
measures:
  - name: order_count
    expr: count(1)
  - name: total_revenue
    expr: sum(revenue)
"""
        util.write_file(updated_metric_view, "models", "test_metric_view.sql")

        # Second run should update via ALTER
        results = util.run_dbt(["run", "--models", "test_metric_view"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the metric view works with new definition
        metric_view_name = f"{project.database}.{project.test_schema}.test_metric_view"
        query_result = project.run_sql(
            f"""
            SELECT
                status,
                order_date,
                MEASURE(order_count) as count,
                MEASURE(total_revenue) as revenue
            FROM {metric_view_name}
            GROUP BY status, order_date
            ORDER BY status, order_date
        """,
            fetch="all",
        )

        assert len(query_result) == 3


class BaseUpdateMetricViewTblProperties(BaseUpdateMetricView):
    def test_metric_view_update_tblproperties(self, project):
        """Test that metric view tblproperties can be updated via ALTER"""
        # First run creates the metric view with tags
        util.run_dbt(["run"])

        # Update with tblproperties added
        updated_metric_view = """
{{
  config(
    materialized='metric_view',
    databricks_tags={
      'team': 'analytics',
      'environment': 'test'
    },
    tblproperties={
      'quality': 'gold'
    }
  )
}}

version: 0.1
source: "{{ ref('source_orders') }}"
dimensions:
  - name: status
    expr: status
measures:
  - name: order_count
    expr: count(1)
"""
        util.write_file(updated_metric_view, "models", "test_metric_view.sql")

        # Second run should update via ALTER
        results = util.run_dbt(["run", "--models", "test_metric_view"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify tblproperties were set
        results = project.run_sql(
            f"show tblproperties {project.database}.{project.test_schema}.test_metric_view",
            fetch="all",
        )

        # Check that 'quality' property exists
        tblprops = {row[0]: row[1] for row in results}
        assert tblprops.get("quality") == "gold"


class BaseUpdateMetricViewTags(BaseUpdateMetricView):
    def test_metric_view_update_tags(self, project):
        """Test that metric view tags can be updated via ALTER"""
        # First run creates the metric view with initial tags
        util.run_dbt(["run"])

        # Update the tags
        updated_metric_view = """
{{
  config(
    materialized='metric_view',
    databricks_tags={
      'team': 'data-science',
      'environment': 'test',
      'priority': 'high'
    }
  )
}}

version: 0.1
source: "{{ ref('source_orders') }}"
dimensions:
  - name: status
    expr: status
measures:
  - name: order_count
    expr: count(1)
"""
        util.write_file(updated_metric_view, "models", "test_metric_view.sql")

        # Second run should update via ALTER
        results = util.run_dbt(["run", "--models", "test_metric_view"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify tags were updated
        results = project.run_sql(
            f"""
            SELECT TAG_NAME, TAG_VALUE FROM {project.database}.information_schema.table_tags
            WHERE schema_name = '{project.test_schema}' AND table_name = 'test_metric_view'
            ORDER BY TAG_NAME
            """,
            fetch="all",
        )

        # Check that we have all three tags
        tags = {row[0]: row[1] for row in results}
        assert tags.get("team") == "data-science"
        assert tags.get("environment") == "test"
        assert tags.get("priority") == "high"


class BaseUpdateMetricViewNothing(BaseUpdateMetricView):
    """Test that no-op updates work correctly"""

    def test_metric_view_update_nothing(self, project):
        """Test that metric view with no changes doesn't error"""
        # First run creates the metric view
        util.run_dbt(["run"])

        # Second run with no changes - should be no-op
        results = util.run_dbt(["run", "--models", "test_metric_view"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the metric view still works
        metric_view_name = f"{project.database}.{project.test_schema}.test_metric_view"
        query_result = project.run_sql(
            f"""
            SELECT
                status,
                MEASURE(order_count) as count
            FROM {metric_view_name}
            GROUP BY status
            ORDER BY status
        """,
            fetch="all",
        )

        assert len(query_result) == 2


# Test classes with view_update_via_alter enabled
@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaAlterQuery(BaseUpdateMetricViewQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaAlterTblProperties(BaseUpdateMetricViewTblProperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaAlterTags(BaseUpdateMetricViewTags):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaAlterNothing(BaseUpdateMetricViewNothing):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }


# Test classes WITHOUT view_update_via_alter (default replace behavior)
@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaReplaceQuery(BaseUpdateMetricViewQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": False,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaReplaceTblProperties(BaseUpdateMetricViewTblProperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": False,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateMetricViewViaReplaceTags(BaseUpdateMetricViewTags):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": False,
            },
        }
