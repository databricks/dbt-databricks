import pytest
from dbt.tests import util

from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalSkipConfigChanges:
    """Test that incremental_apply_config_changes=false skips metadata fetch queries in V1 path."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "v1_skip_config_changes_sql.sql": fixtures.v1_skip_config_changes_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "fail_if_metadata_fetched.sql": fixtures.fail_if_metadata_fetched_macros,
        }

    def test_incremental_run_skips_metadata_queries(self, project):
        # First run creates the table
        util.run_dbt(["run"])
        # Second run exercises the incremental merge path.
        # If metadata fetch macros are called, they will raise errors and the run will fail.
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalColumnMasksNotApplied:
    """Test that column masks are NOT applied in V1 incremental path.

    Column masks must only be applied in V2 where the empty table is created before data
    arrives. In V1 (CTAS), data is written immediately, so applying masks after the fact
    would leave a window where data is unmasked — a security/privacy vulnerability.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "v1_column_mask_model_sql.sql": fixtures.v1_column_mask_model_sql,
            "schema.yml": fixtures.v1_column_mask_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "fail_if_column_masks_applied.sql": fixtures.fail_if_column_masks_applied_macro,
        }

    def test_column_masks_not_applied_in_v1(self, project):
        # Create the mask function so the model config is valid
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION
                {project.database}.{project.test_schema}.full_mask(val STRING)
            RETURNS STRING
            RETURN '*****';
            """
        )

        # First run creates the table
        util.run_dbt(["run"])
        # Second run exercises the incremental merge path with config change detection.
        # If apply_column_masks is called, the overridden macro raises an error.
        util.run_dbt(["run"])
