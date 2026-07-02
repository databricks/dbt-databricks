import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import (
    MaterializationV2Mixin,
    fail_if_tag_and_column_tag_fetch_called_macros,
)
from tests.functional.adapter.tags import fixtures


class BaseTableMetadataFetch:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": fail_if_tag_and_column_tag_fetch_called_macros}


@pytest.mark.skip_profile("databricks_cluster")
class TestTableMetadataFetchSkips(BaseTableMetadataFetch):
    """A tag-free table must never fetch tag metadata."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_table.sql": fixtures.metadata_fetch_table_sql,
            "schema.yml": fixtures.metadata_fetch_no_tags_schema,
        }

    def test_table_runs_never_fetch_tags(self, project):
        # No tags configured, so neither the create nor the re-run fetches.
        util.run_dbt(["run"])
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestTableMetadataFetchRequiresTableTags(BaseTableMetadataFetch):
    """A re-run with table tags fetches to diff against the server."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_table.sql": fixtures.metadata_fetch_table_sql,
            "schema.yml": fixtures.metadata_fetch_table_tags_schema,
        }

    def test_rerun_fetches_table_tags(self, project):
        # Fresh create applies all tags; the re-run fetches to diff.
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]
        assert result.status == RunStatus.Error
        assert "tags should not be called" in result.message


@pytest.mark.skip_profile("databricks_cluster")
class TestTableMetadataFetchRequiresColumnTags(BaseTableMetadataFetch):
    """A re-run with column tags fetches to diff per column."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_table.sql": fixtures.metadata_fetch_table_sql,
            "schema.yml": fixtures.metadata_fetch_column_tags_schema,
        }

    def test_rerun_fetches_column_tags(self, project):
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]
        assert result.status == RunStatus.Error
        assert "tags should not be called" in result.message


@pytest.mark.skip_profile("databricks_cluster")
class TestTableDropRecreateAppliesAllTags(BaseTableMetadataFetch):
    """A dropped+recreated relation inherits no tags, so all are applied (no diff, no fetch)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"metadata_fetch_table.sql": fixtures.metadata_fetch_view_first_sql}

    def test_drop_recreate_applies_all_tags_without_fetch(self, project):
        # View (no fetch) reconfigured to a tagged table: the recreate applies all tags, no fetch.
        util.run_dbt(["run"])
        util.write_file(
            fixtures.metadata_fetch_table_with_tags_sql, "models", "metadata_fetch_table.sql"
        )
        util.run_dbt(["run"])
        results = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags`"
            " where schema_name = '{schema}' and table_name='metadata_fetch_table'",
            fetch="all",
        )
        assert set((row[0], row[1]) for row in results) == {("classification", "internal")}


@pytest.mark.skip_profile("databricks_cluster")
class TestTableMetadataFetchRequiresColumnTagsV2(MaterializationV2Mixin, BaseTableMetadataFetch):
    """Same on the v2 create_table_at path."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_table.sql": fixtures.metadata_fetch_table_sql,
            "schema.yml": fixtures.metadata_fetch_column_tags_schema,
        }

    def test_rerun_fetches_column_tags(self, project):
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]
        assert result.status == RunStatus.Error
        assert "tags should not be called" in result.message
