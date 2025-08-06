import pytest, os
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

MODEL__BASIC_ICEBERG_TABLE = """
{{ config(materialized='table', catalog_name='a-different-catalog') }}
select 1 as id, 'test' as name
"""

MODEL__ICEBERG_TABLE_WITH_LOCATION = """
{{ config(
    materialized='table', 
    catalog_name='a-different-catalog', 
) }}
select 2 as id, 'location_test' as name
"""

MODEL__REF_ICEBERG_TABLE = """
{{ config(materialized='table') }}
select * from {{ ref('basic_iceberg_table') }}
where id = 1
"""

ALT_CATALOG_NAME = os.getenv("DBT_DATABRICKS_ALT_CATALOG")


@pytest.mark.skip_profile("databricks_cluster")
class TestUnityCatalogIntegration(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "a-different-catalog",
                    "active_write_integration": ALT_CATALOG_NAME,
                    "write_integrations": [
                        {
                            "name": ALT_CATALOG_NAME,
                            "catalog_type": "unity",
                            "table_format": "iceberg",
                            "file_format": "delta",
                        }
                    ],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_iceberg_table.sql": MODEL__BASIC_ICEBERG_TABLE,
            "iceberg_table_with_location.sql": MODEL__ICEBERG_TABLE_WITH_LOCATION,
            "ref_iceberg_table.sql": MODEL__REF_ICEBERG_TABLE,
        }

    def test_unity_catalog_iceberg_integration(self, project):
        """Test that Unity Catalog can create and reference Iceberg tables"""
        # Run all models
        run_results = run_dbt(["run", "--log-level", "debug"])

        # Verify all models ran successfully
        assert len(run_results) == 3  # type: ignore

        # Get the expected catalog name from environment variable
        expected_catalog = os.getenv("DBT_DATABRICKS_ALT_CATALOG")
        assert expected_catalog, "DBT_DATABRICKS_ALT_CATALOG environment variable must be set"

        # Verify tables were created in the expected catalog
        expected_tables = [
            "basic_iceberg_table",
            "iceberg_table_with_location",
        ]

        for table_name in expected_tables:
            # Query system information schema to verify table exists in expected catalog
            result = project.run_sql(
                "select table_catalog, table_type from `system`.`information_schema`.`tables` "
                f"where table_catalog = '{expected_catalog}' "
                f"and table_schema = '{project.test_schema}' "
                f"and table_name = '{table_name}'",
                fetch="one",
            )

            # Assert table exists in expected catalog
            assert result is not None, f"Table {table_name} not found in catalog {expected_catalog}"
            actual_catalog, table_type = result
            assert (
                actual_catalog == expected_catalog
            ), f"Table {table_name} found in catalog {actual_catalog}, expected {expected_catalog}"

            # For iceberg tables, verify they have the correct table type
            if "iceberg" in table_name.lower() or table_name == "basic_iceberg_table":
                assert table_type in [
                    "MANAGED",
                    "EXTERNAL",
                ], f"Expected table type MANAGED or EXTERNAL for {table_name}, got {table_type}"

        # Verify we can run again (idempotency)
        run_results = run_dbt(["run"])
        assert len(run_results) == 3  # type: ignore
