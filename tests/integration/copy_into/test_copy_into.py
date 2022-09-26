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


class TestCopyInto(DBTIntegrationTest):
    @property
    def schema(self):
        return "copy_into"

    @property
    def models(self):
        return "models"

    def test_copy_into(self):
        self.run_dbt(["run"])
        # Get the location of the source table.
        rows = self.run_sql(
            "describe table extended {database_schema}.source", fetch="all"
        )
        path = None
        for row in rows:
            if row.col_name == "Location":
                path = row.data_type
        if path is None:
            raise Exception("No location found for the source table")
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
    def test_databricks_cluster(self):
        self.test_copy_into()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_copy_into()
