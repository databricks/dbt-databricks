"""Test for capitalization handling in materialization v2."""

import pytest
from dbt.tests import util

from tests.functional.adapter.basic import fixtures


class TestCapitalizationMismatchV2:
    """
    Test that materialization v2 uses actual table capitalization from the database
    rather than the capitalization from the YAML file.

    Scenario:
    1. Create a table with specific capitalization (e.g., MyTable)
    2. Reference it in YAML with different capitalization (e.g., mytable)
    3. Run dbt again to ensure it recognizes and uses the existing table's capitalization
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
                {{ config(materialized='table') }}
                select 1 as id, 'test' as name
            """
        }

    def test_capitalization_mismatch_on_rerun(self, project):
        """Test that v2 materialization handles capitalization correctly."""
        # First run - create the table
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Verify the table exists
        result = project.run_sql("select count(*) from my_model", fetch="all")
        assert result[0][0] == 1

        # Manually create a table with different capitalization to simulate
        # the scenario where the database has one capitalization but YAML has another
        project.run_sql("drop table if exists MyModel")
        project.run_sql(
            """
            create or replace table MyModel as
            select 1 as id, 'test' as name
            """
        )

        # Drop the lowercase version
        project.run_sql("drop table if exists my_model")

        # Second run - should recognize MyModel as the existing relation
        # even though the YAML defines it as my_model
        # This simulates the case where the database returns "MyModel"
        # but the project expects "my_model"
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # The table should still exist (with either capitalization)
        # Databricks should be case-insensitive for table lookups
        result = project.run_sql("select count(*) from my_model", fetch="all")
        assert result[0][0] == 1


class TestCapitalizationWithAlias:
    """
    Test capitalization handling when using aliases.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
                {{ config(
                    materialized='table',
                    alias='CustomTableName'
                ) }}
                select 1 as id, 'test' as name
            """
        }

    def test_alias_capitalization(self, project):
        """Test that aliases with custom capitalization work correctly."""
        # First run - create with alias
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Table should be accessible by the alias name (case-insensitive)
        result = project.run_sql("select count(*) from CustomTableName", fetch="all")
        assert result[0][0] == 1

        # Second run - should recognize existing table and not fail
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Data should still be correct
        result = project.run_sql("select count(*) from customtablename", fetch="all")
        assert result[0][0] == 1


class TestColumnCapitalizationFromSQL:
    """
    Test that column capitalization comes from SQL SELECT, not from YAML.
    This is the key fix - columns should preserve their case from the SELECT statement.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mixed_case_columns.sql": fixtures.mixed_case_columns_sql,
            "schema.yml": fixtures.mixed_case_columns_schema_yml,
        }

    def test_column_names_preserve_sql_capitalization(self, project):
        """Test that columns in CREATE TABLE use SQL capitalization, not YAML."""
        # Run dbt
        util.run_dbt(["run"])

        # Get the actual column names from the database
        # SQL has: ID, UserName, Age_Value
        # YAML has: id, username, age_value (all lowercase)
        # Databricks preserves the capitalization from the CREATE TABLE statement
        columns_result = project.run_sql("describe mixed_case_columns", fetch="all")

        # Extract column names from DESCRIBE output
        column_names = [row[0] for row in columns_result if not row[0].startswith("#")]

        # Sort for consistent comparison
        column_names_sorted = sorted(column_names)

        # We expect the exact capitalization from the SQL SELECT statement:
        # ID, UserName, Age_Value (sorted: Age_Value, ID, UserName)
        # NOT the YAML capitalization: id, username, age_value
        expected = ["Age_Value", "ID", "UserName"]
        assert column_names_sorted == expected, (
            f"Expected columns from SQL: {expected}, but got: {column_names_sorted}"
        )
