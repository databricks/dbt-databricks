import pytest

from dbt.tests import util


class TestMergeCaseSensitiveColumns:
    """Test case for column name case sensitivity bug in merge operations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "case_sensitive_expected.csv": """id,Name,AGE
1,Alice,25
2,Bob,30
3,Charlie,35""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "case_sensitive_merge.sql": """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

{% if not is_incremental() %}
    select 1 as id, 'Alice' as Name, 25 as AGE
    union all
    select 2 as id, 'Bob' as Name, 30 as AGE
{% else %}
    -- This should update Bob's age and add Charlie
    -- But if column comparison is case sensitive, it might fail to match columns
    select 2 as id, 'Bob' as Name, 30 as AGE  -- existing record
    union all
    select 3 as id, 'Charlie' as Name, 35 as AGE  -- new record
{% endif %}
            """,
        }

    def test_merge_with_capitalized_columns(self, project):
        """Test that merge works when select statement uses capitalized column names."""
        # First run - create initial table
        util.run_dbt(["run"])

        # Check that the initial table was created with mixed case columns
        results = project.run_sql("select count(*) from case_sensitive_merge", fetch="all")
        assert results[0][0] == 2  # Should have 2 rows initially

        # Second run - should perform merge and add new record
        util.run_dbt(["run"])

        # Check that merge worked correctly
        results = project.run_sql("select count(*) from case_sensitive_merge", fetch="all")
        assert results[0][0] == 3  # Should have 3 rows after merge

        # Verify the data matches expected
        util.check_relations_equal(
            project.adapter, ["case_sensitive_merge", "case_sensitive_expected"]
        )


class TestInsertIntoCaseSensitiveColumns:
    """Test case for column name case sensitivity bug in insert_into operations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "insert_case_sensitive_expected.csv": """id,Name,AGE
1,Alice,25
2,Bob,30
1,Alice,25
2,Bob,30""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "insert_case_sensitive.sql": """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='append'
) }}

select 1 as id, 'Alice' as Name, 25 as AGE
union all
select 2 as id, 'Bob' as Name, 30 as AGE
            """,
        }

    def test_insert_with_capitalized_columns(self, project):
        """Test that insert operations work when select statement uses capitalized column names."""
        # First run - create initial table
        util.run_dbt(["run"])

        # Check that the initial table was created
        results = project.run_sql("select count(*) from insert_case_sensitive", fetch="all")
        assert results[0][0] == 2  # Should have 2 rows initially

        # Second run - should append the same data again
        util.run_dbt(["run"])

        # Check that append worked correctly
        results = project.run_sql("select count(*) from insert_case_sensitive", fetch="all")
        assert results[0][0] == 4  # Should have 4 rows after append

        # Verify the data matches expected
        util.check_relations_equal(
            project.adapter, ["insert_case_sensitive", "insert_case_sensitive_expected"]
        )
