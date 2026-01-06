import pytest
from dbt.tests.util import run_dbt, write_file

from tests.functional.adapter.row_filters.fixtures import (
    base_model_mv,
    base_model_sql,
    base_model_streaming_table,
    model_no_filter,
    model_updated_filter,
    model_with_row_filter,
    row_filter_seed,
    view_model_sql,
)
from tests.functional.adapter.fixtures import MaterializationV1Mixin, MaterializationV2Mixin


class BaseRowFilterMixin:
    """Base mixin with helper methods for row filter tests.

    Does not include materialization version - subclasses should combine
    with MaterializationV1Mixin or MaterializationV2Mixin as needed.
    """

    def create_filter_udfs(self, project):
        """Create test UDFs for row filtering."""
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION {project.database}.{project.test_schema}.region_filter(
                region STRING
            )
            RETURNS BOOLEAN
            RETURN region = 'region_a'
            """
        )
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION {project.database}.{project.test_schema}.user_filter(
                user_id STRING
            )
            RETURNS BOOLEAN
            RETURN user_id = 'user1'
            """
        )

    def get_row_filters(self, project, table_name):
        """Query INFORMATION_SCHEMA for row filters.

        Uses catalog-specific path and includes table_catalog filter
        for multi-catalog safety.
        """
        return project.run_sql(
            f"""
            SELECT filter_name FROM {project.database}.information_schema.row_filters
            WHERE table_catalog = '{project.database}'
              AND table_schema = '{project.test_schema}'
              AND table_name = '{table_name}'
            """,
            fetch="all",
        )


class RowFilterMixin(BaseRowFilterMixin, MaterializationV2Mixin):
    """Row filter mixin for V2 materialization tests."""

    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestRowFilterTable(RowFilterMixin):
    """Test row filters on table models."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql,
            "schema.yml": model_with_row_filter,
        }

    def test_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle."""
        # Setup
        self.create_filter_udfs(project)

        # 1. Create with filter (tests unqualified function qualification)
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. Update filter
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 3. Remove filter
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRowFilter(RowFilterMixin):
    """Test row filters on incremental models."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "incremental"),
            "schema.yml": model_with_row_filter,
        }

    def test_incremental_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle on incremental model."""
        self.create_filter_udfs(project)

        # 1. CREATE with filter (initial run)
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. Incremental run should preserve filter (no config change)
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 3. UPDATE filter (incremental run with config change)
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 4. REMOVE filter (incremental run removing config)
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewRowFilter(RowFilterMixin):
    """Test row filters on materialized view models."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_mv,
            "schema.yml": model_with_row_filter,
        }

    def test_mv_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle on MV."""
        self.create_filter_udfs(project)

        # 1. CREATE with filter
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. UPDATE filter
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 3. DROP filter
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableRowFilter(RowFilterMixin):
    """Test row filters on streaming table models."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base_model_seed.csv": row_filter_seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_streaming_table,
            "schema.yml": model_with_row_filter,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_streaming_table_seed(self, project):
        """Run seed once for the entire class to avoid Delta table ID conflicts."""
        run_dbt(["seed"])

    @pytest.fixture(scope="function", autouse=True)
    def cleanup_streaming_table(self, project):
        project.run_sql(
            f"DROP TABLE IF EXISTS {project.database}.{project.test_schema}.base_model"
        )
        yield

    def test_streaming_table_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle on ST."""
        self.create_filter_udfs(project)

        # 1. CREATE with filter
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. UPDATE filter
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 3. DROP filter
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


@pytest.mark.skip_profile("databricks_cluster")
class TestViewRowFilterFailure(MaterializationV2Mixin):
    """Test that row filters on regular views fail with clear error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": view_model_sql,
            "schema.yml": model_with_row_filter,
        }

    def test_view_row_filter_failure(self, project):
        """Verify row filters on views fail appropriately."""
        result = run_dbt(["run"], expect_pass=False)
        assert result.results[0].status != "success"
        # Note: The exact error depends on whether it's compile-time or runtime
        # For views, the WITH ROW FILTER syntax itself may fail


# ============================================================================
# SAFE_RELATION_REPLACE PATH TEST
# ============================================================================


@pytest.mark.skip_profile("databricks_cluster")
class TestRowFilterTableSafeReplace(RowFilterMixin):
    """Test row filters on table models with safe_relation_replace path."""

    @pytest.fixture(scope="class")
    def models(self):
        from tests.functional.adapter.row_filters.fixtures import base_model_safe_sql

        return {
            "base_model.sql": base_model_safe_sql,
            "schema.yml": model_with_row_filter,
        }

    def test_safe_replace_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle with safe_relation_replace."""
        self.create_filter_udfs(project)

        # 1. Create with filter (initial run - no existing relation)
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. Update filter (existing relation - should take SAFE_RELATION_REPLACE path)
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 3. Remove filter (existing relation - should take SAFE_RELATION_REPLACE path)
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


# ============================================================================
# V1 MATERIALIZATION TESTS
# ============================================================================


@pytest.mark.skip_profile("databricks_cluster")
class TestRowFilterTableV1(BaseRowFilterMixin, MaterializationV1Mixin):
    """Test row filters on table models with V1 materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql,
            "schema.yml": model_with_row_filter,
        }

    def test_row_filter_lifecycle(self, project):
        """Test create, update, and remove row filter lifecycle in V1."""
        self.create_filter_udfs(project)

        # 1. Create with filter
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "region_filter" in filters[0][0].lower()

        # 2. Update filter
        write_file(model_updated_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
        assert "user_filter" in filters[0][0].lower()

        # 3. Remove filter
        write_file(model_no_filter, "models", "schema.yml")
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 0


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRowFilterV1(BaseRowFilterMixin, MaterializationV1Mixin):
    """Test row filters on incremental models with V1 materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "incremental"),
            "schema.yml": model_with_row_filter,
        }

    def test_incremental_row_filter(self, project):
        """Test row filter on incremental model lifecycle in V1."""
        self.create_filter_udfs(project)

        # Initial run
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1

        # Incremental run should preserve filter
        run_dbt(["run"])
        filters = self.get_row_filters(project, "base_model")
        assert len(filters) == 1
