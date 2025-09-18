import pytest

from tests.unit.macros.base import MacroTestBase


class TestGetCreateMetricViewAsSQL(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/metric_view"]

    def test_basic_metric_view_creation(self, template_bundle):
        """Test that get_create_metric_view_as_sql generates correct Databricks SQL"""
        yaml_spec = """version: 0.1
source: orders
dimensions:
  - name: order_date
    expr: order_date
measures:
  - name: order_count
    expr: count(1)"""

        result = self.run_macro_raw(
            template_bundle.template,
            "databricks__get_create_metric_view_as_sql",
            template_bundle.relation,
            yaml_spec
        )


        expected = """create or replace view `some_database`.`some_schema`.`some_table`
with metrics
language yaml
as $$
version: 0.1
source: orders
dimensions:
  - name: order_date
    expr: order_date
measures:
  - name: order_count
    expr: count(1)
$$"""

        # For metric views, we need to preserve YAML formatting exactly
        assert result.strip() == expected.strip()

    def test_metric_view_with_filter(self, template_bundle):
        """Test metric view generation with filter clause"""
        yaml_spec = """version: 0.1
source: orders
filter: status = 'completed'
dimensions:
  - name: order_date
    expr: order_date
measures:
  - name: revenue
    expr: sum(amount)"""

        result = self.run_macro_raw(
            template_bundle.template,
            "databricks__get_create_metric_view_as_sql",
            template_bundle.relation,
            yaml_spec
        )

        expected = """create or replace view `some_database`.`some_schema`.`some_table`
with metrics
language yaml
as $$
version: 0.1
source: orders
filter: status = 'completed'
dimensions:
  - name: order_date
    expr: order_date
measures:
  - name: revenue
    expr: sum(amount)
$$"""

        assert result.strip() == expected.strip()

    def test_complex_metric_view(self, template_bundle):
        """Test metric view with multiple dimensions and measures"""
        yaml_spec = """version: 0.1
source: customer_orders
filter: order_date >= '2024-01-01'
dimensions:
  - name: customer_segment
    expr: customer_type
  - name: order_month
    expr: date_trunc('MONTH', order_date)
measures:
  - name: total_orders
    expr: count(1)
  - name: total_revenue
    expr: sum(order_total)
  - name: avg_order_value
    expr: avg(order_total)"""

        result = self.run_macro_raw(
            template_bundle.template,
            "databricks__get_create_metric_view_as_sql",
            template_bundle.relation,
            yaml_spec
        )

        # Check that all key parts are present
        assert "create or replace view" in result.lower()
        assert "with metrics" in result.lower()
        assert "language yaml" in result.lower()
        assert "as $$" in result.lower()
        assert result.strip().endswith("$$")
        assert "version: 0.1" in result
        assert "source: customer_orders" in result
        assert "filter: order_date >= '2024-01-01'" in result
        assert "customer_segment" in result
        assert "total_revenue" in result

    def test_generic_macro_dispatcher(self, template_bundle):
        """Test that the generic get_create_metric_view_as_sql macro works"""
        yaml_spec = """version: 0.1
source: test_table
measures:
  - name: count
    expr: count(1)"""

        # Mock the adapter dispatch to return our databricks implementation
        template_bundle.context["adapter"].dispatch.return_value = getattr(
            template_bundle.template.module, "databricks__get_create_metric_view_as_sql"
        )

        result = self.run_macro_raw(
            template_bundle.template,
            "get_create_metric_view_as_sql",
            template_bundle.relation,
            yaml_spec
        )

        # Should generate the same output as the databricks-specific macro
        assert "create or replace view" in result.lower()
        assert "with metrics" in result.lower()
        assert "language yaml" in result.lower()
        assert "version: 0.1" in result
        assert "source: test_table" in result