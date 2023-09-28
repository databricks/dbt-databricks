import os
import pytest
from tests.integration.base import DBTIntegrationTest, use_profile


@pytest.mark.skip(reason="Multi catalog not created by DECO team yet (March 2023)")
class TestMultiCatalog(DBTIntegrationTest):
    setup_alternate_db = True

    @property
    def schema(self):
        return "multi_catalog"

    @property
    def models(self):
        return "models"

    @property
    def alternative_database(self):
        return os.getenv("DBT_DATABRICKS_UC_ALTERNATIVE_CATALOG", "main")

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "models": {"materialized": "table"},
        }

    def test_multi_catalog_run(self, seed_catalog):
        self.run_dbt(["seed"])

        self.assertEqual(len(self.run_dbt(["run"])), 3)
        self.assertEqual(len(self.run_dbt(["run"])), 3)

        self.assertManyRelationsEqual(
            [
                ("seed", self.unique_schema(), seed_catalog),
                ("alternative_catalog", self.unique_schema(), self.alternative_database),
                ("refer_alternative_catalog", self.unique_schema(), self.default_database),
                ("cross_catalog", self.unique_schema(), self.default_database),
            ]
        )

        self.run_dbt(["snapshot"])
        self.run_dbt(["snapshot"])

        results = self.run_sql(
            "select * from {database_schema}.my_snapshot",
            fetch="all",
            kwargs=dict(database=self.alternative_database),
        )
        self.assertEqual(len(results), 2)

        catalog = self.run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 5


@pytest.mark.skip(reason="Multi catalog not created by DECO team yet (March 2023)")
class TestMultiCatalogTableModels(TestMultiCatalog):
    @use_profile("databricks_uc_cluster")
    def test_multi_catalog_run_databricks_uc_cluster(self):
        self.test_multi_catalog_run(self.default_database)

    @use_profile("databricks_uc_sql_endpoint")
    def test_multi_catalog_run_databricks_uc_sql_endpoint(self):
        self.test_multi_catalog_run(self.default_database)


@pytest.mark.skip(reason="Multi catalog not created by DECO team yet (March 2023)")
class TestMultiCatalogViewModels(TestMultiCatalog):
    @property
    def project_config(self):
        return {
            "config-version": 2,
            "models": {"materialized": "view"},
        }

    @use_profile("databricks_uc_cluster")
    def test_multi_catalog_run_databricks_uc_cluster(self):
        self.test_multi_catalog_run(self.default_database)

    @use_profile("databricks_uc_sql_endpoint")
    def test_multi_catalog_run_databricks_uc_sql_endpoint(self):
        self.test_multi_catalog_run(self.default_database)


@pytest.mark.skip(reason="Multi catalog not created by DECO team yet (March 2023)")
class TestMultiCatalogSeedsInAlternativeCatalog(TestMultiCatalog):
    @property
    def project_config(self):
        return {
            "config-version": 2,
            "seeds": {
                "catalog": self.alternative_database,
            },
        }

    @use_profile("databricks_uc_cluster")
    def test_multi_catalog_run_databricks_uc_cluster(self):
        self.test_multi_catalog_run(self.alternative_database)

    @use_profile("databricks_uc_sql_endpoint")
    def test_multi_catalog_run_databricks_uc_sql_endpoint(self):
        self.test_multi_catalog_run(self.alternative_database)
