import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter.column_masks.fixtures import (
    base_model_sql,
    model,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class TestColumnMask(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql,
            "schema.yml": model,
        }

    def test_column_mask(self, project):
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
            """,
            fetch="all",
        )

        assert len(masks) == 1
        assert masks[0][0] == "password"  # column_name
        assert masks[0][1] == f"{project.database}.{project.test_schema}.password_mask"  # mask_name

        # Verify masked value
        result = project.run_sql("SELECT id, password FROM base_model", fetch="one")
        assert result[0] == "abc-123"
        assert result[1] == "*****"  # Masked value should be 5 asterisks


class TestIncrementalColumnMask(TestColumnMask):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "incremental"),
            "schema.yml": model,
        }
