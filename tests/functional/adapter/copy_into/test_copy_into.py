import pytest
from dbt.tests import util

from tests.functional.adapter.copy_into import fixtures


class BaseCopyInto:
    args_formatter = ""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": fixtures.source,
            "expected_target.csv": fixtures.expected_target,
            "expected_target_expression_list.csv": fixtures.expected_target_expression_list,
            "seed_schema.yml": fixtures.seed_schema,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"target.sql": fixtures.target, "schema.yml": fixtures.model_schema}

    def path(self, project):
        util.run_dbt(["seed"])

        # Get the location of the source table.
        rows = util.run_sql_with_adapter(
            project.adapter, "describe table extended {schema}.source", fetch="all"
        )
        path = None
        for row in rows:
            if row.col_name == "Location":
                path = row.data_type
        if path is None:
            raise Exception("No location found for the source table")
        return path

    def copy_into(self, path, args_formatter):
        util.run_dbt(["run"])
        util.run_dbt(
            [
                "run-operation",
                "databricks_copy_into",
                "--args",
                args_formatter.format(source_path=path),
            ]
        )


# TODO: figure out a way to test this on UC
@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestCopyInto(BaseCopyInto):
    args_formatter = """
target_table: target
source: {source_path}
file_format: parquet
format_options:
  mergeSchema: 'true'
copy_options:
  mergeSchema: 'true'
"""

    def test_copy_into(self, project):
        path = self.path(project)
        self.copy_into(path, self.args_formatter)
        util.check_relations_equal(project.adapter, ["target", "expected_target"])


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestCopyIntoWithExpressionList(BaseCopyInto):
    args_formatter = """
target_table: target
source: {source_path}
expression_list: 'id, name'
file_format: parquet
format_options:
  mergeSchema: 'true'
copy_options:
  mergeSchema: 'true'
"""

    def test_copy_into_with_expression_list(self, project, path):
        path = self.path(project)
        self.copy_into(path, self.args_formatter)
        util.check_relations_equal(project.adapter, ["target", "expected_target_expression_list"])
