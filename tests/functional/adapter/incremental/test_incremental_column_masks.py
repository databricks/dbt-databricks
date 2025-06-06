import pytest

from dbt.tests import util
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalColumnMasks(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "column_mask_sql.sql": fixtures.column_mask_sql,
            "schema.yml": fixtures.column_mask_base,
        }

    def test_changing_column_masks(self, project):
        # Create the full mask function
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION
                {project.database}.{project.test_schema}.full_mask(password STRING)
            RETURNS STRING
            RETURN '*****';
            """
        )
        # Masks all characters before the @ symbol
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION
                {project.database}.{project.test_schema}.email_mask(value STRING)
            RETURNS STRING
            RETURN CONCAT(
                REPEAT('*', POSITION('@' IN value) - 1),
                SUBSTR(value, POSITION('@' IN value))
            );
            """
        )

        # First run with name masked
        util.run_dbt(["run"])
        masks = project.run_sql(
            "SELECT id, name, email, password FROM column_mask_sql",
            fetch="all",
        )
        assert len(masks) == 1
        assert masks[0][0] == 1
        assert masks[0][1] == "*****"  # name (masked)
        assert masks[0][2] == "*****"  # email (masked)
        assert masks[0][3] == "password123"  # password (unmasked)

        # Update masks and verify changes
        util.write_file(fixtures.column_mask_valid_mask_updates, "models", "schema.yml")
        util.run_dbt(["run"])

        result = project.run_sql(
            "SELECT id, name, email, password FROM column_mask_sql", fetch="all"
        )
        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][1] == "hello"  # name (unmasked)
        assert result[0][2] == "********@example.com"  # email (partially masked)
        assert result[0][3] == "*****"  # password (masked)
