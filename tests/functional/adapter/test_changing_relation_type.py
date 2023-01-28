import pytest

from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator


class TestChangeRelationTypesDatabricks(BaseChangeRelationTypeValidator):
    pass


@pytest.mark.skip_profile(
    "databricks_uc_cluster", "databricks_sql_endpoint", "databricks_uc_sql_endpoint"
)
class TestChangeRelationTypesParquetDatabricks(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            }
        }
