import pytest

from dbt.tests import util
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalColumnTags(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.column_tags_a,
        }

    def test_changing_column_tags(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.column_tags_b, "models", "schema.yml")
        util.run_dbt(["run"])

        # Check final column tags
        results = project.run_sql(
            f"""
            select column_name, tag_name, tag_value
            from `system`.`information_schema`.`column_tags`
            where schema_name = '{project.test_schema}'
            and table_name = 'merge_update_columns'
            order by column_name, tag_name
            """,
            fetch="all",
        )

        assert len(results) == 3

        # Convert to dict for easier assertions
        tags_dict = {}
        for row in results:
            col = row.column_name
            if col not in tags_dict:
                tags_dict[col] = {}
            tags_dict[col][row.tag_name] = row.tag_value

        # Verify expected final state
        assert tags_dict["id"] == {"pii": "false", "source": "application"}
        assert tags_dict["msg"] == {"pii": "true"}


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalPythonColumnTags(TestIncrementalColumnTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns.py": fixtures.column_tags_python_model,
            "schema.yml": fixtures.column_tags_a,
        }
