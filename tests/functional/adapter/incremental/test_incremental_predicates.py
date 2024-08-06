import pytest

from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from tests.functional.adapter.incremental import fixtures


class TestIncrementalPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                fixtures.models__databricks_incremental_predicates_sql
            )
        }


class TestPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                fixtures.models__databricks_incremental_predicates_sql
            )
        }
