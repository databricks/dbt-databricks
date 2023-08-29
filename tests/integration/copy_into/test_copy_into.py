from tests.integration.base import DBTIntegrationTest, use_profile


args = """
target_table: target
source: {source_path}
file_format: parquet
format_options:
  mergeSchema: 'true'
copy_options:
  mergeSchema: 'true'
"""

args_with_expression_list = """
target_table: target_with_expression_list
source: {source_path}
expression_list: 'id, name'
file_format: parquet
format_options:
  mergeSchema: 'true'
copy_options:
  mergeSchema: 'true'
"""


class TestCopyInto(DBTIntegrationTest):
    @property
    def schema(self):
        return "copy_into"

    @property
    def models(self):
        return "models"

    def prepare(self):
        self.run_dbt(["run"])
        # Get the location of the source table.
        rows = self.run_sql("describe table extended {database_schema}.source", fetch="all")
        path = None
        for row in rows:
            if row.col_name == "Location":
                path = row.data_type
        if path is None:
            raise Exception("No location found for the source table")
        return path

    @use_profile("databricks_cluster")
    def test_copy_into_databricks_cluster(self):
        path = self.prepare()

        self.run_dbt(
            [
                "run-operation",
                "databricks_copy_into",
                "--args",
                args.format(source_path=path),
            ]
        )
        self.assertTablesEqual("target", "expected_target")

    @use_profile("databricks_cluster")
    def test_copy_into_with_expression_list_databricks_cluster(self):
        path = self.prepare()

        self.run_dbt(
            [
                "run-operation",
                "databricks_copy_into",
                "--args",
                args_with_expression_list.format(source_path=path),
            ]
        )
        self.assertTablesEqual(
            "target_with_expression_list", "expected_target_with_expression_list"
        )
