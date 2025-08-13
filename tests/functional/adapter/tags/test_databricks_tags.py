import pytest

from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.tags import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestTags:
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.tags_sql.replace("table", self.materialized),
        }

    def test_tags(self, project):
        _ = util.run_dbt(["run", "--models", "tags"])
        _ = util.run_dbt(["run", "--models", "tags"])
        results = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags`"
            " where schema_name = '{schema}' and table_name='tags'",
            fetch="all",
        )
        assert len(results) == 2
        expected_tags = {("a", "b"), ("c", "d")}
        actual_tags = set((row[0], row[1]) for row in results)
        assert actual_tags == expected_tags


@pytest.mark.skip_profile("databricks_cluster")
class TestTagsUpdateViaAlter(MaterializationV2Mixin):
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.tags_sql.replace("table", self.materialized),
        }

    def test_updated_tags(self, project):
        util.run_dbt(["run"])
        util.write_file(
            fixtures.updated_tags_sql.replace("table", self.materialized),
            "models",
            "tags.sql",
        )
        util.run_dbt(["run"])
        results = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags`"
            " where schema_name = '{schema}' and table_name='tags'",
            fetch="all",
        )
        assert len(results) == 3
        expected_tags = {("a", "b"), ("c", "d"), ("e", "f")}
        actual_tags = set((row[0], row[1]) for row in results)
        assert actual_tags == expected_tags


@pytest.mark.skip_profile("databricks_cluster")
class TestViewTags(TestTags):
    materialized = "view"


@pytest.mark.skip_profile("databricks_cluster")
class TestViewTagsUpdateViaAlter(TestTagsUpdateViaAlter):
    materialized = "view"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalTags(TestTags):
    materialized = "incremental"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalTagsUpdateViaAlter(TestTagsUpdateViaAlter):
    materialized = "incremental"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewTags(TestTags):
    materialized = "materialized_view"


@pytest.mark.skip_profile("databricks_cluster")
class TestMaterializedViewTagsUpdateViaAlter(TestTagsUpdateViaAlter):
    materialized = "materialized_view"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableTags(TestTags):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.streaming_table_tags_sql,
        }

    def test_tags(self, project):
        util.run_dbt(["seed"])
        super().test_tags(project)


@pytest.mark.skip_profile("databricks_cluster")
class TestStreamingTableTagsUpdateViaAlter:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.streaming_table_tags_sql,
        }

    def test_updated_tags(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        util.write_file(
            fixtures.updated_streaming_table_tags_sql,
            "models",
            "tags.sql",
        )
        util.run_dbt(["run"])
        results = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags`"
            " where schema_name = '{schema}' and table_name='tags'",
            fetch="all",
        )
        assert len(results) == 3


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.py": fixtures.simple_python_model,
            "schema.yml": fixtures.python_schema,
        }
