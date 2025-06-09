import pytest

from dbt.tests.util import run_dbt, write_file
from tests.functional.adapter.column_masks.fixtures import (
    base_model_sql,
    base_model_streaming_table,
    column_mask_seed,
    model,
    model_with_extra_args,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin


@pytest.mark.skip_profile("databricks_cluster")
class TestColumnMask(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql,
            "schema.yml": model,
        }

    def test_column_mask_no_extra_args(self, project):
        # Create the mask function
        project.run_sql(
            f"CREATE OR REPLACE FUNCTION {project.database}.{project.test_schema}."
            "password_mask(password STRING) RETURNS STRING RETURN '*****';"
        )

        run_dbt(["run"])

        # Verify column mask was created
        masks = project.run_sql(
            f"""
            SELECT column_name, mask_name
            FROM {project.database}.information_schema.column_masks
            WHERE table_schema = '{project.test_schema}'
            """,
            fetch="all",
        )

        assert len(masks) == 1
        assert masks[0][0] == "password"  # column_name
        assert masks[0][1] == f"{project.database}.{project.test_schema}.password_mask"  # mask_name

        # Verify masked value
        result = project.run_sql("SELECT id, password FROM base_model", fetch="one")
        assert result[0] == "abc123"
        assert result[1] == "*****"  # Masked value should be 5 asterisks

    def test_column_mask_with_extra_args(self, project):
        write_file(model_with_extra_args, "models", "schema.yml")

        # Create a mask function that concatenates all possible arg types: original column, other
        # columns, and every supported literal type from
        # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-column-mask
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION {project.database}.{project.test_schema}.weird_mask(
                password STRING,
                id STRING,
                literal STRING,
                num INT,
                bool_val BOOLEAN,
                null_val STRING,
                interval INTERVAL DAY
            )
            RETURNS STRING
            RETURN CONCAT(
                password, '-',
                id, '-',
                literal, '-',
                CAST(num AS STRING), '-',
                CAST(bool_val AS STRING), '-',
                COALESCE(null_val, 'NULL'), '-',
                CAST(interval AS STRING)
            );
            """
        )
        run_dbt(["run"])

        # Not meant to resemble a real life example. Just for the sake of testing different types
        result = project.run_sql("SELECT id, password FROM base_model", fetch="one")
        assert result[0] == "abc123"
        assert result[1] == "password123-abc123-literal_string-333-true-NULL-INTERVAL '2' DAY"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalColumnMask(TestColumnMask):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "incremental"),
            "schema.yml": model,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestStreamingTableColumnMask(TestColumnMask):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base_model_seed.csv": column_mask_seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_streaming_table,
            "schema.yml": model,
        }

    def test_streaming_table_column_mask_with_extra_args(self, project):
        run_dbt(["seed"])
        super().test_column_mask_with_extra_args(project)


@pytest.mark.skip_profile("databricks_cluster")
class TestViewColumnMaskFailure(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "view"),
            "schema.yml": model,
        }

    def test_view_column_mask_failure(self, project):
        result = run_dbt(["run"], expect_pass=False)
        assert "Column masks are not supported" in result.results[0].message


@pytest.mark.skip_profile("databricks_cluster")
class TestMaterializedViewColumnMaskFailure(TestViewColumnMaskFailure):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "materialized_view"),
            "schema.yml": model,
        }
