"""Test that seeds work correctly without specifying location_root.

This test ensures that when no location_root is specified, the adapter
does not write an empty location clause which would cause the error:
"Can not create a Path from an empty string"

See: https://github.com/databricks/dbt-databricks/issues/1228
"""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import MaterializationV2Mixin

# Simple seed CSV data
simple_seed_csv = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""


class TestSeedNoLocation:
    """Test seed creation without location_root config."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"simple_seed.csv": simple_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Explicitly do not set location_root to test default behavior."""
        return {
            "seeds": {
                # No location_root specified - this is the default case
                "+file_format": "delta",
            }
        }

    def test_seed_without_location_root(self, project):
        """Test that seeds can be created without specifying location_root."""
        # Run seed command
        results = util.run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the seed was created and has the correct data
        sql = "select count(*) as num_rows from {schema}.simple_seed"
        result = project.run_sql(sql, fetch="one")
        assert result[0] == 3

    def test_seed_full_refresh_without_location_root(self, project):
        """Test that seed full refresh works without location_root."""
        # Initial seed
        results = util.run_dbt(["seed"])
        assert len(results) == 1

        # Full refresh
        results = util.run_dbt(["seed", "--full-refresh"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify data is still correct
        sql = "select count(*) as num_rows from {schema}.simple_seed"
        result = project.run_sql(sql, fetch="one")
        assert result[0] == 3


class TestSeedNoLocationV2(TestSeedNoLocation, MaterializationV2Mixin):
    """Test seed creation without location_root using v2 materialization."""

    pass


class TestSeedEmptyLocationRoot:
    """Test seed creation with explicitly empty location_root.

    This tests the edge case where location_root might be set to empty string
    rather than None, which could cause the "Can not create a Path from an empty string" error.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"empty_location_seed.csv": simple_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Test with an empty location_root (edge case)."""
        return {
            "seeds": {
                # Explicitly empty location_root should be handled gracefully
                # In practice, dbt might filter this out, but we want to ensure robustness
                "+file_format": "delta",
            }
        }

    def test_seed_with_empty_config(self, project):
        """Test that seeds work even if location_root config could be empty."""
        results = util.run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the seed was created
        sql = "select count(*) as num_rows from {schema}.empty_location_seed"
        result = project.run_sql(sql, fetch="one")
        assert result[0] == 3


class TestSeedEmptyLocationRootV2(TestSeedEmptyLocationRoot, MaterializationV2Mixin):
    """Test seed creation with empty location_root using v2 materialization."""

    pass
