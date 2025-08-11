import pytest

from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED
from tests.functional.adapter.tags import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.tags_sql,
        }

    def test_tags(self, project):
        _ = util.run_dbt(["run"])
        _ = util.run_dbt(["run"])
        results = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags`"
            " where schema_name = '{schema}' and table_name='tags'",
            fetch="all",
        )
        assert len(results) == 2


@pytest.mark.skip_profile("databricks_cluster")
class TestViewTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {"tags.sql": fixtures.tags_sql.replace("table", "view")}


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {"tags.sql": fixtures.tags_sql.replace("table", "incremental")}


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.sql": fixtures.tags_sql.replace("table", "materialized_view"),
        }


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


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestPythonTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.py": fixtures.simple_python_model,
            "schema.yml": fixtures.python_schema,
        }
