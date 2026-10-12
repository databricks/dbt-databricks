import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import (
    fail_if_column_tag_fetch_called_macros,
)
from tests.functional.adapter.incremental import fixtures


class IncrementalMetadataFetchSkipsMixin:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_no_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "fail_if_column_tag_fetch_called.sql": fail_if_column_tag_fetch_called_macros,
            "record_constraint_fetches.sql": fixtures.record_constraint_fetches_macros,
        }

    def test_second_incremental_run_succeeds_without_unneeded_metadata_fetches(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["run"])
        project.run_sql("create or replace table metadata_constraint_fetches (fetch_name string)")
        util.run_dbt(["run"])

        fetches = project.run_sql(
            "select fetch_name from metadata_constraint_fetches order by fetch_name",
            fetch="all",
        )
        rows = project.run_sql("select id from metadata_fetch_incremental", fetch="all")
        assert fetches == []
        assert rows == [(1,)]


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchSkips(IncrementalMetadataFetchSkipsMixin):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "use_materialization_v2": False,
                "use_describe_as_json_for_relation_metadata": False,
            }
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchSkipsV2(IncrementalMetadataFetchSkipsMixin):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "use_materialization_v2": True,
                "use_describe_as_json_for_relation_metadata": False,
            }
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchRequiresTableTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_table_tags_schema,
        }

    def test_second_incremental_run_reads_table_tags_from_api(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() fetches tags.
        util.run_dbt(["run"])
        util.run_dbt(["run"])

        tags = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags` "
            "where schema_name = '{schema}' "
            "and table_name = 'metadata_fetch_incremental'",
            fetch="all",
        )
        assert {(row[0], row[1]) for row in tags} == {("classification", "internal")}


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchRequiresColumnTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_column_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_column_tag_fetch_called.sql": fail_if_column_tag_fetch_called_macros}

    def test_second_incremental_run_fails_when_column_tag_fetch_is_required(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]

        assert result.status == RunStatus.Error
        assert "tags should not be called" in result.message


class TaggedAliasMixin:
    alias: str

    @pytest.fixture(scope="class")
    def models(self):
        return {"tagged_alias.sql": fixtures.metadata_fetch_tagged_alias_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"tagged_alias": self.alias}}

    def test_second_incremental_run_preserves_rows_and_tags(self, project):
        util.run_dbt(["run"])
        util.run_dbt(["run"])

        rows = project.run_sql(f"select id from `{self.alias}` order by id", fetch="all")
        tags = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags` "
            f"where catalog_name = '{project.database}' "
            "and schema_name = '{schema}' "
            f"and table_name = '{self.alias}'",
            fetch="all",
        )
        assert rows == [(1,), (2,)]
        assert tags == [("classification", "internal")]


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalTableTagsHashAlias(TaggedAliasMixin):
    alias = "tagged_alias#2026"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalTableTagsQueryAlias(TaggedAliasMixin):
    alias = "tagged_alias?2026"
