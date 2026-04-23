import pytest
from dbt.tests import util

from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalColumnTags:
    """Test that V1 incremental path applies column tag changes via process_config_changes."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "v1_config_changes_sql.sql": fixtures.v1_config_changes_sql,
            "schema.yml": fixtures.v1_column_tags_a,
        }

    def test_changing_column_tags(self, project):
        # First run creates the table
        util.run_dbt(["run"])

        # Update column tags
        util.write_file(fixtures.v1_column_tags_b, "models", "schema.yml")
        util.run_dbt(["run"])

        # Verify column tags were applied
        results = project.run_sql(
            f"""
            select column_name, tag_name, tag_value
            from `system`.`information_schema`.`column_tags`
            where schema_name = '{project.test_schema}'
            and table_name = 'v1_config_changes_sql'
            order by column_name, tag_name
            """,
            fetch="all",
        )

        tags_dict = {}
        for row in results:
            col = row.column_name
            if col not in tags_dict:
                tags_dict[col] = {}
            tags_dict[col][row.tag_name] = row.tag_value

        # Verify expected final state
        expected_tags = {
            "id": {"pii": "true"},
            "msg": {"source": "app"},
        }
        assert tags_dict == expected_tags


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalColumnMasks:
    """Test that V1 incremental path applies column mask changes via process_config_changes."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "column_mask_sql.sql": fixtures.column_mask_sql,
            "schema.yml": fixtures.column_mask_base,
        }

    def test_changing_column_masks(self, project):
        # Create mask functions
        project.run_sql(
            f"""
            CREATE OR REPLACE FUNCTION
                {project.database}.{project.test_schema}.full_mask(password STRING)
            RETURNS STRING
            RETURN '*****';
            """
        )
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

        # First run with initial masks
        util.run_dbt(["run"])
        masks = project.run_sql(
            "SELECT id, name, email, password FROM column_mask_sql",
            fetch="all",
        )
        assert len(masks) == 1
        assert masks[0][1] == "*****"  # name (masked)
        assert masks[0][3] == "password123"  # password (unmasked)

        # Update masks and verify changes
        util.write_file(fixtures.column_mask_valid_mask_updates, "models", "schema.yml")
        util.run_dbt(["run"])

        result = project.run_sql(
            "SELECT id, name, email, password FROM column_mask_sql", fetch="all"
        )
        assert len(result) == 1
        assert result[0][1] == "hello"  # name (unmasked)
        assert result[0][3] == "*****"  # password (masked)


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalSkipConfigChanges:
    """Test that incremental_apply_config_changes=false skips metadata fetch queries."""

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
