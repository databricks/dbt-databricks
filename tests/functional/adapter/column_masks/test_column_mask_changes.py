import pytest
from dbt.tests.util import run_dbt, write_file

from tests.functional.adapter.column_masks.fixtures import (
    base_model_sql,
    model,
    model_no_mask,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin, RerunSafeMixin


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalColumnMaskRetained(RerunSafeMixin, MaterializationV2Mixin):
    """Dropping column_mask from the model leaves the existing mask in place, so masks
    applied outside dbt survive incremental runs."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql.replace("table", "incremental"),
            "schema.yml": model,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("base_model",)

    def _column_masks(self, project):
        return project.run_sql(
            f"""
            SELECT column_name, mask_name
            FROM {project.database}.information_schema.column_masks
            WHERE table_schema = '{project.test_schema}'
              AND table_name = 'base_model'
            """,
            fetch="all",
        )

    def test_removing_column_mask_keeps_it(self, project):
        project.run_sql(
            f"CREATE OR REPLACE FUNCTION {project.database}.{project.test_schema}."
            "password_mask(password STRING) RETURNS STRING RETURN '*****';"
        )

        # First run: the mask is applied to the column at create time.
        run_dbt(["run"])
        masks = self._column_masks(project)
        assert len(masks) == 1
        assert masks[0][0] == "password"
        assert project.run_sql("SELECT password FROM base_model", fetch="one")[0] == "*****"

        # Drop the column_mask from the model and re-run against the existing relation.
        write_file(model_no_mask, "models", "schema.yml")
        run_dbt(["run"])

        masks_after = self._column_masks(project)
        assert len(masks_after) == 1
        assert masks_after[0][0] == "password"
        assert project.run_sql("SELECT password FROM base_model", fetch="one")[0] == "*****"


@pytest.mark.skip_profile("databricks_cluster")
class TestTableColumnMaskRetained(TestIncrementalColumnMaskRetained):
    """Dropping column_mask from a table model leaves the existing mask in place across
    CREATE OR REPLACE rebuilds."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": base_model_sql,
            "schema.yml": model,
        }
