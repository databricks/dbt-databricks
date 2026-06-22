import pytest
from dbt.tests import util

from tests.functional.adapter.column_tags import fixtures
from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    MaterializationV2Mixin,
    RerunSafeMixin,
)


class ColumnTagsMixin(RerunSafeMixin, MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("base_model",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.initial_column_tag_model.replace(
                "materialized: table", f"materialized: {self.relation_type}"
            ),
        }

    def test_column_tags(self, project):
        util.run_dbt(["run"])

        column_tags_query = f"""
            SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{project.database}'
              AND schema_name = '{project.test_schema}'
              AND table_name = 'base_model'
            ORDER BY column_name, tag_name
            """

        # Check that column tags were applied
        tags = project.run_sql(column_tags_query, fetch="all")
        expected_tags = {
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
            ("account_number", "key_only", ""),
            ("account_number", "null_value", ""),
        }
        actual_tags = {(row[0], row[1], row[2]) for row in tags}
        assert actual_tags == expected_tags

        # Run a second time with an updated model
        util.write_file(
            fixtures.updated_column_tag_model.replace(
                "materialized: table", f"materialized: {self.relation_type}"
            ),
            "models",
            "schema.yml",
        )
        util.run_dbt(["run"])

        # Check that column tags were updated
        tags = project.run_sql(column_tags_query, fetch="all")
        expected_tags = {
            ("id", "pii", "false"),
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
            ("account_number", "key_only", ""),
            ("account_number", "null_value", ""),
        }
        actual_tags = {(row[0], row[1], row[2]) for row in tags}
        assert actual_tags == expected_tags


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsTable(ColumnTagsMixin):
    relation_type = "table"


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsIncremental(ColumnTagsMixin):
    relation_type = "incremental"


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestColumnTagsMaterializedView(ColumnTagsMixin):
    relation_type = "materialized_view"


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableColumnTags(ColumnTagsMixin):
    relation_type = "streaming_table"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base_model_seed.csv": fixtures.column_tags_seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_streaming_table,
            "schema.yml": fixtures.initial_column_tag_model.replace(
                "materialized: table", "materialized: streaming_table"
            ),
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_streaming_table_seed(self, project):
        util.run_dbt(["seed"])

    @pytest.fixture(autouse=True)
    def stop_streaming_query(self, project):
        # Drop base_model on teardown to stop its streaming query promptly, so it
        # can't orphan and cascade failures into other tests on the same xdist worker.
        yield
        self._drop_relations(project, ("base_model",))


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsView(ColumnTagsMixin):
    relation_type = "view"


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsTableV1(ColumnTagsMixin):
    relation_type = "table"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Override MaterializationV2Mixin to test the V1 (default) materialization path
        return {}


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsIncrementalV1(ColumnTagsMixin):
    relation_type = "incremental"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Override MaterializationV2Mixin to test the V1 (default) materialization path.
        # Exercises both the V1 incremental create path (run 1) and the V1 merge-time
        # configuration changeset (run 2 adds a tag to a previously-untagged column).
        return {}


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsIncrementalV1FullRefresh(RerunSafeMixin, MaterializationV1Mixin):
    """A `dbt run --full-refresh` of a V1 incremental model must re-apply column tags.

    Full-refresh recreates the relation from scratch (the recreate branch of the V1
    incremental materialization), so any column tags must be re-applied there or they
    are lost on every full refresh.
    """

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("base_model",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.initial_column_tag_model.replace(
                "materialized: table", "materialized: incremental"
            ),
        }

    def _column_tags(self, project):
        rows = project.run_sql(
            f"""
            SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{project.database}'
              AND schema_name = '{project.test_schema}'
              AND table_name = 'base_model'
            ORDER BY column_name, tag_name
            """,
            fetch="all",
        )
        return {(row[0], row[1], row[2]) for row in rows}

    def test_column_tags_survive_full_refresh(self, project):
        expected = {
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
            ("account_number", "key_only", ""),
            ("account_number", "null_value", ""),
        }
        # Initial run creates the relation (create path applies the tags).
        util.run_dbt(["run"])
        assert self._column_tags(project) == expected
        # Full refresh recreates the relation; the recreate path must re-apply the tags.
        util.run_dbt(["run", "--full-refresh"])
        assert self._column_tags(project) == expected
