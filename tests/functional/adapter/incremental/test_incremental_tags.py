import pytest
from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


class TestIncrementalPersistDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.tags_a,
        }

    def test_changing_tags(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.tags_b, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "select tag_name, tag_value from {database}.information_schema.table_tags "
            "where schema_name = '{schema}' and table_name='merge_update_columns_sql'",
            fetch="all",
        )
        assert len(results) == 2
        results_dict = {}
        results_dict[results[0].tag_name] = results[0].tag_value
        results_dict[results[1].tag_name] = results[1].tag_value
        assert results_dict == {"c": "e", "d": "f"}
