import pytest
from dbt.tests import util
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
            "select tag_name, tag_value from {database}.information_schema.table_tags "
            "where schema_name = '{schema}' and table_name='tags'",
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


@pytest.mark.skip_profile("databricks_cluster")
class TestPythonTags(TestTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tags.py": fixtures.simple_python_model,
            "schema.yml": fixtures.python_schema,
        }
