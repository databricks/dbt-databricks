import pytest

from dbt.tests import util
from tests.functional.adapter.column_tags import fixtures
from tests.functional.adapter.fixtures import MaterializationV2Mixin


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTags(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.model_with_column_tags,
        }

    def test_column_tags(self, project):
        util.run_dbt(["run"])

        # Check that column tags were applied
        tags = project.run_sql(
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

        expected_tags = {
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
        }

        actual_tags = {(row[0], row[1], row[2]) for row in tags}
        assert actual_tags == expected_tags


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsIncremental(TestColumnTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.model_with_column_tags.replace(
                "materialized: table", "materialized: incremental"
            ),
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnTagsMaterializedView(TestColumnTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.model_with_column_tags.replace(
                "materialized: table", "materialized: materialized_view"
            ),
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestViewColumnTagsFailure(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.model_with_column_tags.replace(
                "materialized: table", "materialized: view"
            ),
        }

    def test_view_column_tags_failure(self, project):
        result = util.run_dbt(["run"], expect_pass=False)
        assert "Column tags are not supported" in result.results[0].message


@pytest.mark.skip_profile("databricks_cluster")
class TestStreamingTableColumnTagsFailure(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": fixtures.base_model_sql,
            "schema.yml": fixtures.model_with_column_tags.replace(
                "materialized: table", "materialized: streaming_table"
            ),
        }

    def test_streaming_table_column_tags_failure(self, project):
        result = util.run_dbt(["run"], expect_pass=False)
        assert "Column tags are not supported" in result.results[0].message
